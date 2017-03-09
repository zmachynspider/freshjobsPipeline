from bs4 import BeautifulSoup
import re
import requests
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import boto3
from datetime import datetime
import json
import sys


def isJob(element):
    '''Checks if the html link is a job element, return Bool'''
    try:
        if element['data-tn-element'] == 'jobTitle':
            return True
        else:
            return False
    except:
        return False


def getJob(element):
    '''If the html link is a job element, returns the role title'''
    try:
        if isJob(element):
            return element['title'].strip()
        else:
            return None
    except:
        return None


def getJobLink(element):
    '''If the html link is a job element, returns the link to the job posting page'''
    try:
        if isJob(element):
            return element['href'].strip()
        else:
            return None
    except:
        return None


def dump_html(bucket, path, page):
    '''Ingests an S3 bucket, the name of the file and the content of the page and uploads to S3'''
    output_file = bucket.new_key(path)
    output_file.content_type = 'text/html'
    output_file.set_contents_from_string(page, policy='public-read')
    return None


if __name__ == "__main__":

#   compile all your regex patterns
    job_pattern = re.compile('''(?:data-tn-element="jobTitle")
                            ([\s\S]*?)
                            (?:(?:(?:<span\sitemprop="addressLocality">)|
                            (?:<span\sclass="location">))
                            ([\s\S]*?)(?:span))''', re.X)

    company_pattern = re.compile('''(?:<span\sclass="company")
                                (?:(?:>\n<a\sclass(?:[\s\S]*?)target="_blank">)|
                                (?:\sitemprop="hiringOrganization"))?
                                ([\s\S]*?)(?:</a>)?</span>''', re.X)

    role_pattern = re.compile(r'''(?:^>([\s\S]*?)</a>)|
                                  (?:^(?:[\s\S]*?)title="
                                  ([\s\S]*?)")''', re.X)

    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

#   create a url to query
    role = sys.argv[1].replace('_','+')
#   role = 'data+scientist'
    location = sys.argv[2].replace('_','+').replace(',','%2C')
#   location = 'Bay+Area'

    url_list = ['http://www.indeed.com/jobs?as_and=',
                role,
                '&as_phr=&as_any=&as_not=&as_ttl=&as_cmp=&jt=all&st=&salary=&radius=25&filter=0&l=',
                location,
                '&fromage=1&limit=1000&sort=date&psf=advsrch']

    url = ''.join(url_list)
    r = requests.get(url, headers=headers)
    page = r.text

#   connect to s3
    conn = S3Connection()
    bucket = conn.get_bucket('deproj')

#   create the next path
    path = (
#   start out in the folder we want
            'html/' + datetime.now().strftime('%Y') +
#           add the month
                '/' + datetime.now().strftime('%m') +
#               add the day
                '/' + datetime.now().strftime('%d') +
#               add the hour
                '/' + datetime.now().strftime('%H') +
#               add the title
                '/' + role.replace('+',"").lower() +
#               add the location
                '/' + location.replace('+',"").replace('%2C', "").lower() +
                '/'
            )

#   save the jobs postings page in S3
    dump_html(bucket,
              path +
                'indeed-posting' +
                datetime.now().strftime("%Y-%m-%d-%H-%M-%S") +
                '.html',
              page)

#   pull out job postings
    matches = job_pattern.findall(page)

#   pull down the links:
    jobs_links = []
    soup = BeautifulSoup(page, "html.parser")
    for link in soup.findAll('a', href=True):
        if isJob(link):
            jobs_links.append((getJob(link), getJobLink(link)))

    counter = 0
    error_counter = 0
    client = boto3.client('firehose','us-east-1')
    for i, match in enumerate(matches):
        try:
#           fetch the job location
            location = match[1].split('<')[0]

#    fetch the company
            submatch = company_pattern.search(match[0]).group(1)
            company = submatch.split('>\n')[-1].strip().replace('<b>', '').replace('</b>', '')

            jobs = role_pattern.search(match[0]).groups()

#           get the job
            for job in list(jobs):
                if job:
                    job = job.replace('<b>','').replace('</b>', '')
                    final_job = job.strip()

#                   get the link:
#   check that the job titles match from the html and the links... if they don't
            while (jobs_links[counter][0].strip()[0:50] != final_job[0:50]) and (counter < len(jobs_links)-1):
                counter += 1

#               check that we didn't run down our counter and we're not at the end of our loop
            if (counter == len(jobs_links)-1) and (i < len(matches)-1):
#               then reset the counter and decide that the link is NA
                job_link = None
                counter = i
                job_posting = None
            else:
#               print jobs_links[counter][1]
                job_link = jobs_links[counter][1]
                r2 = requests.get('http://www.indeed.com' + job_link, headers=headers)
                job_posting = r2.text

#               don't forget to add the file into our S3 store
                dump_html(bucket,
                  path +
                    job_link[0:250].replace('/','-') +
                    '/' +
                    'job-posting' +
                    datetime.now().strftime("%Y-%m-%d-%H-%M-%S") +
                    '.html',
                  job_posting)

#                 parse out the text out of the webpage
                job_posting = BeautifulSoup(job_posting, "html.parser")
                for elem in job_posting.find_all(['script', 'style']):
                    elem.extract()
                job_posting = job_posting.getText()

            key = {'title': final_job,
                   'company': company,
                   'location': location,
                   'link': job_link[0:250],
                   'date': datetime.now().strftime("%Y-%m-%d"),
                   'text': job_posting.replace('\n',' ').replace('\r', ' ').replace('\t', ' ')}

            client.put_record(DeliveryStreamName='indeed_json',
                                      Record = {'Data': json.dumps(key,
                                                                   ensure_ascii=False).encode('utf8') + '\n'})

        except:
            error_counter += 1
            continue
    print "error count is :" + str(error_counter)
