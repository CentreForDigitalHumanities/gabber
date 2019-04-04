## Gabber - data-analysis tools for gab.ai

This repository aims to provide a set of tools for data-driven media studies on the gab.ai platform.

### Requirements

These tools require python3 and access to a MongoDB server.
On a debian system, run:

    sudo apt-get install python3-pymongo mongodb-server

### Scraping

The minegab.py script is meant for scraping data from the gab.ai platform. All scraped data is stored in MongoDB for further parsing/analysis.

#### Usage

Scraping data from gab.ai starts at a particular account, whose username has to be manually provided to the script:

    ./minegab.py -u <username>

From there, the script will discover other accounts through reposts, follow-relations, comments, and quotes.
Once the first account has been processed, the -a parameter will tell the script to scrape data from all the discovered accounts. In doing so, more accounts will likely be discovered:

    ./minegab.py -a

Keep running the script with -a until no new accounts are discovered. The giant graph within gab.ai has now been scraped.
The minegab.py will give verbose output with the -d flag. Note that this might contain special characters that could be problematic to print on your terminal:

    export PYTHONIOENCODING=UTF-8
    ./minegab.py -da

To keep a logfile of the scraping, you could use the following command:

    ./minegab.py -a | tee -a ./scrapelog.txt

To redo scraping of accounts, first remove the account from the profiles collection, and then scrape it again:

    ./minegab.py -d <username> ; ./minegab.py -u <username>

To scrape the news section, simply run:

    ./minegab.py -n

#### Performance

Performance will increase when multiple scrapers are run simultaneously. Ideally, the scrapers would use different outbound IP addresses to decrease the impact of rate-limiting, but performance is already greatly improved when running multiple scrapes from the same node. Note that running scrapers from multiple nodes requires replication of the MongoDB backend.

#### Limitations

The minegab.py script can not scrape beyond the giant graph of which the manually provided accounts are a part. It will not find other communities if they are completely isolated from the accounts provided to the script.

Furthermore, the minegab.py script does not retrieve any media content. It will store links to media assets in the database, which could be used as an input for a downloading script, but this functionality is not provided by the script. Note that scraping all media content will require considerable bandwidth and storage capacity.

Finally, the 'groups' section of gab is mostly ignored. Group metadata is shown in the posts, but group membership is not scraped.


### Processing

#### Groups

The gabgroups.py script will gather all group metadata found in the scraped posts and fill a mongo collection named groups. It will also add a post count to the metadata.

By default, gabgroups.py will only consider original posts. Use the -r parameter to also include reposts in the gathering of groups and counting of posts.


### Exporting

#### Groups

The groups2csv.py script will export group metadata to a csv file. Use -o <filename> to export to a specific filename, by default the export will be written to gabgroups.csv.

The format of the export is comma separated and single quote delimited CSV.

#### Hashtags

The gabhashtags.py script will export a sorted list of all hashtags used in posts and comments on gab, including a count of how often they were used. Use -o <filename> to export to a specific filename, by default the export will be written to gabhashtags.csv.

The format of the export is comma separated and single quote delimited CSV.

Note that no weighing is applied in the hashtag count.
