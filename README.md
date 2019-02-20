## Gabber - data-analysis tools for gab.ai

This repository aims to provide a set of tools for data-driven media studies on the gab.ai platform.

### Requirements

These tools require python3 and access to a MongoDB server.
On a debian system, run:

    sudo apt-get install python3-pymongo mongodb-server

### Scraping

The minegab.py script is meant for scraping data from the gab.ai platform.
Scraping data from gab.ai starts at a particular account, whose username has to be manually provided to the script:

    ./minegab.py -u <username>

From there, the script will discover other accounts through reposts, follow-relations, comments, and quotes.
Once the first account has been processed, the -a parameter will tell the script to scrape data from all the discovered accounts. In doing so, more accounts will likely be discovered:

    ./minegab.py -a

Keep running the script with -a untill no new accounts are discovered. The giant graph within gab.ai has now been scraped.
The minegab.py will give verbose output with the -d flag. Note that this might contain special characters that could be problematic to print on your terminal:

    export PYTHONIOENCODING=UTF-8
    ./minegab.py -da

### Processing

TBD.
