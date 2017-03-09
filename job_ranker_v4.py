from pyspark.sql.functions import UserDefinedFunction, split, row_number,desc, col, current_date, datediff #unix_timestamp
from pyspark.sql.types import StringType
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.window import Window
import re
from scipy import spatial
import numpy as np
import pandas as pd
from markdown import markdown
from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover, NGram
from datetime import datetime
import plotly.plotly as py
import plotly.graph_objs as go
import yaml


def fix_location(s):
    '''ingests a string of location, takes our zip and spaces'''

    s = ''.join(c for c in s if c.isdigit() == False)
    s = s.split(',')
    if len(s) != 2:
        s = ['NA', 'NA']
    return ','.join([c.strip() for c in s])


def clean_text(s):
    """Ingests a string with extra junk and outputs stripped, lower-cased text"""
    pattern = re.compile('[^a-zA-Z]')
    s = pattern.sub(" ", s)
    s = re.sub(' +',' ', s)
    return s.lower()


def compute_similarity(row, vec, beautify=True):
    '''ingests a vector of features for the ideal job and computes the cos similarity score'''
    row = np.array(row)
    vec = np.array(vec)
    cos_sim = 1 - spatial.distance.cosine(row, vec)
    if beautify == False:
        return cos_sim
    else:
        return int(cos_sim * 100)


def get_job_id(df,title,company,state):
    '''ingests the job description that is ideal and returns the job_id that it belongs to'''
    j_id = df.where((col("title") == title) &
                  (col("company") == company) &
                  (col("state") == state)).select("job_id").collect()
    return j_id[0]['job_id']


def classify_title(s):
    '''ingests title string and returns the class it belongs to'''
    if ('Machine Learn' in s) or ('ML' in s):
        return 'ML'
    elif ('Artificial Intel' in s) or ('AI' in s):
        return 'AI'
    elif ('Statist' in s):
        return 'STAT'
    elif ('Analytic' in s):
        return 'ANA'
    elif ('Data Scientist' in s):
        return 'DS'
    elif ('Programmer' in s):
        return 'PRG'
    elif ('Data Engin' in s):
        return 'DE'
    else:
        return 'NA'


def make_line_obj(table, x, yname):
    '''Makes life easier- ingests x & string of y and make the pyplot'''
    obj = go.Scatter(
    x=dates,
    y=table[yname],
    mode = 'lines',
    name = str(yname)
    )

    return obj

def get_top_chart(df, ideal_vecs, top_n=20):
    '''given a pyspark df containing the state jobs listings and numpy arrays of ideal jobs, will sort the df'''
    def helper(row):
        '''Ingests a string title and link and creates a clickable link'''
        return '[' + row['title'] + '](http://www.indeed.com'+ row['link'] + ')'

    features = df.select("features").collect()
    df_vecs = [row['features'] for row in features]
    sims = np.mean([[compute_similarity(vec,i_vec) for i_vec in ideal_vecs] for vec in df_vecs], axis=1)
    sim_order = np.argsort(sims)[::-1]
    df_pd = df.select("job_id","title","company","date","city","state","link").toPandas()
    df_pd = df_pd.set_index(df_pd.job_id)
    del df_pd['job_id']
    df_pd = df_pd.iloc[sim_order,:]
    df_pd['score'] = sims[sim_order]
    #df_pd['link'] = df_pd['link'].apply(lambda x: '[link](http://indeed.com{0})'.format(x))
    #rename the title a clickable link
    df_pd['title'] = df_pd.apply(helper, axis=1)
    del df_pd['link']
#    df_pd.reset_index(drop=True, inplace=True)
    df_pd = df_pd.sort_values(["date","score"], ascending=False)
    df_pd.index = np.arange(1, len(df_pd)+1)
    return df_pd.iloc[:top_n,:]


def send_to_s3(keyname, bucket, html):
    """Checks if the file already exists in the bucket and creates if it does not"""
    key = bucket.new_key(keyname)
    key.content_type = 'text/html'
    key.set_contents_from_string(html, policy='public-read')
    return None


def make_state_page(df, conn, keyname='CA', bucketname='www.jobs.com'):
    '''ingests a table to print do s3 website bucket'''
#   fix issue with printing the entire dataframe
    pd.set_option('display.max_colwidth', -1)
    website_bucket = conn.get_bucket(bucketname)
    html = df.to_html(
        formatters=dict(
            title=markdown
        ),
        escape=False,
        index=True
    ) +" postings last updated "+str(datetime.now().strftime("%Y-%m-%d %H:%M"))
    html = '<!DOCTYPE html><HTML><head><link rel="stylesheet" href="http://s3.amazonaws.com/www.jobs.com/style.css"></head><body>{}</body></HTML>'.format(html.encode('utf8'))
    send_to_s3(keyname=keyname, bucket=website_bucket, html=html)
    return None


if __name__ == "__main__":

#   sc = SQLContext(SparkContext("local[2]", appName="jobRanker"))
    sc = SQLContext(SparkContext())

#   fetch credentials for plotly
#   pull down the credentials from a private s3 key.
#   Use a junk s3 connection that we later want to trash to avoid
#   bug in boto that won't let us connect to a bucket with a period
    conn_0 = S3Connection(host='s3.amazonaws.com')
    bucket = conn_0.get_bucket('deproj')
    pc = bucket.get_key("/emr/plotly_creds.yml")
    creds = yaml.load(pc.get_contents_as_string())
    py.sign_in(creds['plotly']['user'],creds['plotly']['passw'])

    df = sc.read.json("s3://deproj/json/*/*/*/*/")

    df.cache()
#   implement the cache
    df.count()

    name = 'location'

#   create the udf
    loc_udf = UserDefinedFunction(fix_location, StringType())
    new_df = df.select(*[loc_udf(column).alias(name) if column == name
                     else column for column in df.columns])
    w = Window().orderBy("date")

#   add an index for your jobs
    df4 = new_df.withColumn("job_id", row_number().over(w))


#   normalize the job location
    split_col = split(df4['location'], ',')
    df4 = df4.withColumn('city', split_col.getItem(0))
    df4 = df4.withColumn('state', split_col.getItem(1))

#   we want to dedupe our job postings for only the
#   get the first time we saw the job posting
    w = Window().partitionBy(["title", "company", "location"]).orderBy("date")
    df4 = df4.withColumn("unique_id", row_number().over(w))
    first_df = df4.select(col("title").alias("title1"),
                          col("company").alias("company1"),
                          col("location").alias("location1"),
                          col("date").alias("date1"))\
                          .filter(df4.unique_id == 1)


#   now get the latest link & text that belongs to the first posting
    w = Window().partitionBy(["title", "company", "location"])\
                .orderBy(desc("date"))
    df4 = df4.withColumn("unique_id", row_number().over(w))
    last_df = df4.filter(df4.unique_id == 1)


#   join the fist date on the last_df
    df4 = last_df.join(first_df, (last_df.title == first_df.title1) &
                      (last_df.company == first_df.company1) &
                      (last_df.location == first_df.location1),"left_outer")\
                       .select("title",
                               "company",
                               "job_id",
                               col("date1").alias("date"),
                               "location",
                               "city",
                               "state",
                               "link",
                               "text")

    jobs = df4.select(df4.job_id,
                      df4.title,
                      df4.company,
                      df4.state,
                      df4.city,
                      df4.date,
                      df4.link,
                      df4.text)

#   clean the text data of any junk you don't want to be in there
    clean_text_udf = UserDefinedFunction(clean_text, StringType())
    clean_jobs = jobs.withColumn('clean_text', clean_text_udf('text'))

#   cache for ML calculations
    clean_jobs.cache()
    clean_jobs.count()

#   split up into words
    tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")
    wordsData = tokenizer.transform(clean_jobs)
#   remove stopwords
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    filteredWords = remover.transform(wordsData)

#   get ngrams
    ngram = NGram(inputCol="filtered", outputCol="featureGrams")
    gramData = ngram.transform(filteredWords)

#   create TFIDF of these
    hashingTF = HashingTF(inputCol="featureGrams",
                          outputCol="rawFeatures", numFeatures=350)
    featurizedData = hashingTF.transform(gramData)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)

    rescaledData.cache()
    rescaledData.count()

#   now filter all the criteria you care about:
    ds_preds = rescaledData.where((col("title").like("%Machine Learn%")) |
                        (col("title").like("%Data Scientist%")) |
                        (col("title").like("%Artificial Intel%")) |
                       (col("title").like("%Analytic%")) |
                       (col("title").like("%Statist%")) |
                        (col("title").like("%ML%")) |
                        (col("title").like("%AI%")) |
                       (col("title").like("%Data Engin%")) |
                       (col("title").like("%Programmer%"))) #analytics cluster

# Here we have a quick chance to interject our analysis with some EDA charts
#   select data to make your graphs
    state_pd = ds_preds.where((col('state') == 'CA')|
                               (col('state') == 'NY')|
                               (col('state') == 'NJ')|
                               (col('state') == 'WA')|
                               (col('state') == 'OR'))\
                               .select("job_id","date","state","title")

#   fix new jersey
    fix_state_udf = UserDefinedFunction(lambda s: 'NY' if s == 'NJ' else s, StringType())
    state_pd = state_pd.select(*[fix_state_udf(column).alias('state') if column == 'state'
                         else column for column in state_pd.columns])

#   classify titles into buckets
    class_job_udf = UserDefinedFunction(classify_title, StringType())
    state_pd = state_pd.withColumn('title_class', class_job_udf('title'))

#   get simple exponential smoothing for region by date
#   we keep the pyspark crosstab roll up in spark but bring it into Pandas once we have aggregates
    state_by_date = state_pd.crosstab("date", "state")\
                            .toPandas().sort_values('date_state')
    state_es = pd.ewma(state_by_date, span=7)

#   repeat for title analysis
    title_by_date = state_pd.crosstab("date", "title_class")\
                            .toPandas().sort_values('date_title_class')
    title_es = pd.ewma(title_by_date, span=7)

    fix_state_udf = UserDefinedFunction(lambda s: 'NY' if s == 'NJ' else s, StringType())
    state_pd = state_pd.select(*[fix_state_udf(column).alias('state') if column == 'state'
                     else column for column in state_pd.columns])

#   make our state plot
    dates = state_es['date_state']
    state_list = ['CA', 'NY', 'WA', 'OR']

    data = []
    for state in state_list:
        data.append(make_line_obj(state_es, dates, state))

    layout = go.Layout(
        title='Daily Job Postings by Region',
        xaxis=dict(
            title='Day',
            titlefont=dict(
                size=14,
                color='#7f7f7f'
            )
        ),
        yaxis=dict(
            title='Volume',
            titlefont=dict(
                size=14,
                color='#7f7f7f'
            )
        )
    )
    fig = go.Figure(data=data, layout=layout)
    state_url = py.plot(fig, filename='jobs-by-state', auto_open=False)

#   make our title plot
    dates = title_es['date_title_class']
    title_list = ['AI', 'ANA', 'DE', 'DS','ML','PRG','STAT']

    data = []
    for title in title_list:
        data.append(make_line_obj(title_es, dates, title))

    layout = go.Layout(
        title='Daily Job Postings by Job Title',
        xaxis=dict(
            title='Day',
            titlefont=dict(
                size=14,
                color='#7f7f7f'
            )
        ),
        yaxis=dict(
            title='Volume',
            titlefont=dict(
                size=14,
                color='#7f7f7f'
            )
        )
    )
    fig = go.Figure(data=data, layout=layout)
    title_url = py.plot(fig, filename='jobs-by-title', auto_open=False)


#   END of EDA graphics back to filtering our postings
#   filter for only recent postings
    ds_preds_rec = ds_preds.where(datediff(current_date(), col("date")) <= 14)

#   chop up by location
    CA = ds_preds_rec.filter(col('state') == 'CA')
    WA = ds_preds_rec.filter(col('state') == 'WA')
    OR = ds_preds_rec.filter(col('state') == 'OR')
    #NY Area includes nj & ny
    NY = ds_preds_rec.filter((col('state') == 'NY') |(col('state') == 'NJ') )

#   choose postings that you want weighted cosine similarity for
    good_jobs = []
    good_jobs.append(get_job_id(rescaledData,"Data Scientist","PayScale","WA"))
    #good_jobs.append(get_job_id(rescaledData,"ML/AL Scientist","Cyanogen, Inc.","WA"))
    #good_jobs.append(get_job_id(rescaledData,"Summer 2017 Intern, Data Scientist","Salesforce","CA"))

#   get features that belong to those postings
    good_job_vecs = rescaledData.where(col("job_id").isin(good_jobs)).select("job_id", "features").collect()
    vecs = [job['features'] for job in good_job_vecs]

#   pull down pandas dataframe for each region
    ca_pd = get_top_chart(CA, vecs, CA.count())
    ny_pd = get_top_chart(NY, vecs, NY.count())
    wa_pd = get_top_chart(WA, vecs, WA.count())
    or_pd = get_top_chart(OR, vecs, OR.count())

#   set up a new s3 connection because we need to avoid boto bug
    conn = S3Connection(host='s3.amazonaws.com', calling_format=OrdinaryCallingFormat())

#   fix the issue with printing a big tabe to html
#    pd.set_option('display.max_colwidth', -1)

    bucketname ='www.jobs.com'

    make_state_page(ca_pd, conn, 'CA', bucketname)
    make_state_page(ny_pd, conn, 'NY', bucketname)
    make_state_page(wa_pd, conn, 'WA', bucketname)
    make_state_page(or_pd, conn, 'OR', bucketname)
