from boto.s3.connection import S3Connection, OrdinaryCallingFormat

def send_to_s3(keyname, bucket, html, text=True):
    """Checks if the file already exists in the bucket and creates if it does not"""
    key = bucket.new_key(keyname)
    if text:
        key.content_type = 'text/html'
    else:
        key.content_type = 'text/css'
    key.set_contents_from_string(html, policy='public-read')
    return None


if __name__ == "__main__":

    conn = S3Connection(host='s3.amazonaws.com', calling_format=OrdinaryCallingFormat())
    bucketname ='www.jobs.com'

#   error message:
    error_html = """
                <html>
                  <head><title>Website Currently Out of Order</title></head>
                  <body><h2>Please check back in at a later time</h2></body>
                </html>"""

#   check if the error html exists
    bucket = conn.get_bucket(bucketname)

    send_to_s3('error.html', bucket, error_html)

    index_html = """
                <!DOCTYPE html>
                <html>
                <head>
                <style>
                h1 {
                    color: #377ba8;
                    font-family: Lucida Sans Unicode;
                    font-size: 2.1em;
                    border-bottom: 2px solid #eee;
                }
                p  {
                    color: #377ba8;
                    font-family: Lucida Grande;
                    font-size: 0.95em;
                }
                ul  {
                    color: #fff;
                    font-family: Lucida Grande;
                    font-size: 0.95em;
                }
                </style>
                </head>
                <body>

                <h1>Freshjobs</h1>
                <p>Fresh data science job postings from the last two weeks:</p>
                <ul style="list-style-type:disc">
                  <li><a href="http://www.jobs.com.s3-website-us-east-1.amazonaws.com/CA">California</a></li>
                  <li><a href="http://www.jobs.com.s3-website-us-east-1.amazonaws.com/WA">Washington</a></li>
                  <li><a href="http://www.jobs.com.s3-website-us-east-1.amazonaws.com/OR">Oregon</a></li>
                  <li><a href="http://www.jobs.com.s3-website-us-east-1.amazonaws.com/NY">New York</a></li>
                </ul>
            <iframe width="900" height="600" frameborder="0" scrolling="no" src="//plot.ly/~zmachynspider/6.embed"></iframe>
            <iframe width="900" height="600" frameborder="0" scrolling="no" src="//plot.ly/~zmachynspider/8.embed"></iframe>

                </body>
                </html>"""

    send_to_s3('index.html', bucket, index_html)

    bucket.configure_website('index.html', 'error.html')

#   now create the css page to format the linked tables:
    css = '''body            { font-family: "Lucida Sans Unicode", "Lucida Grande", sans-serif;}
            body            { color: #377ba8; }
            body            { margin: 0; }
            body            { font-size: 1.2em; }

            table.dataframe, .dataframe th, .dataframe td {
              border: none;
              border-bottom: 1px solid #C8C8C8;
              border-collapse: collapse;
              text-align:left;
              padding: 8px;
              margin-bottom: 40px;
              font-size: 0.8em;
            }

            tr:nth-child(odd)		{ background-color:#eee; }
            tr:nth-child(even)	{ background-color:#fff; }

            tr:hover            { background-color: #ffff99;}'''

    send_to_s3('style.css', bucket, css, False)
