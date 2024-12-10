import os
import re
import sqlite3
import time
from collections import defaultdict
from porter import PorterStemmer  # Ensure this matches your filename

# Global variables for metrics
tokens = 0
documents = 0
terms = 0

# The database is a simple dictionary to hold term objects
database = {}

# Define a Term class to hold term data
class Term:
    def __init__(self):
        self.termid = 0
        self.termfreq = defaultdict(int)
        self.docs = 0
        self.docids = set()
        self.tf_idf = defaultdict(float)

# Function to split text into tokens
def splitchars(line):
    chars = re.compile(r'\W+')  # Regex to split by non-word characters
    return chars.split(line)

# Function to process each line of a document
def parsetoken(line):
    global documents, tokens, terms
    line = line.replace('\t', ' ').strip()
    l = splitchars(line)
    
    for elmt in l:
        elmt = elmt.replace('\n', '').lower().strip()
        tokens += 1
        
        if elmt not in database:
            terms += 1
            database[elmt] = Term()
            database[elmt].termid = terms
            database[elmt].docids = set()
            database[elmt].docs = 0
        
        if documents not in database[elmt].docids:
            database[elmt].docs += 1
            database[elmt].docids.add(documents)
        
        database[elmt].termfreq[documents] += 1

# Function to process a single file
def process(filename):
    try:
        with open(filename, 'r') as file:
            for line in file.readlines():
                parsetoken(line)
    except IOError:
        print(f"Error opening file {filename}")
        return False
    return True

# Function to walk through directories and process documents
def walkdir(dirname): 
    global documents
    all_files = [f for f in os.listdir(dirname) if os.path.isdir(os.path.join(dirname, f)) or os.path.isfile(os.path.join(dirname, f))]
    for f in all_files:
        if os.path.isdir(os.path.join(dirname, f)): 
            walkdir(os.path.join(dirname, f))
        else:
            documents += 1
            cur.execute("INSERT INTO DocumentDictionary VALUES (?, ?)", (os.path.join(dirname, f), documents)) 
            if not process(os.path.join(dirname, f)):
                documents -= 1

# Function to write the document dictionary to a file
def write_document_dictionary(cursor, output_file="documents_dictionary.txt"):
    with open(output_file, "w") as f:
        f.write("Document Name\tDocument ID\n")
        for row in cursor.execute("SELECT DocumentPath, DocID FROM DocumentDictionary"):
            f.write(f"{row[0]}\t{row[1]}\n")
    print(f"Document Dictionary saved to {output_file}")

# Function to write the term dictionary to a file
def write_term_dictionary(output_file="terms_dictionary.txt"):
    with open(output_file, "w") as f:
        f.write("Term\tTerm ID\n")
        sorted_terms = sorted(database.items(), key=lambda item: item[0])
        for term, term_obj in sorted_terms:
            f.write(f"{term}\t{term_obj.termid}\n")
    print(f"Term Dictionary saved to {output_file}")

if __name__ == '__main__':
    folder = "./CACM"  # Ensure this path matches your document folder

    # SQLite setup
    conn = sqlite3.connect('indexer.db')
    cur = conn.cursor()

    # Create tables
    cur.execute('CREATE TABLE IF NOT EXISTS DocumentDictionary (DocumentPath TEXT, DocID INTEGER)')
    cur.execute('CREATE TABLE IF NOT EXISTS TermDictionary (Term TEXT, TermID INTEGER)')
    cur.execute('CREATE TABLE IF NOT EXISTS Metrics (MetricName TEXT, MetricValue INTEGER)')

    start_time = time.localtime()
    print(f"Start Time: {start_time.tm_hour}:{start_time.tm_min}")
    
    walkdir(folder)  # Perform the indexing

    # Write the Document Dictionary to a file
    write_document_dictionary(cur)
    
    # Write the Term Dictionary to a file
    write_term_dictionary()

    # Print processing statistics
    print(f"Number of documents processed: {documents}")
    print(f"Total terms parsed: {tokens}")
    print(f"Total unique terms: {terms}")

    end_time = time.localtime()
    print(f"End Time: {end_time.tm_hour}:{end_time.tm_min}")

    # Commit and close database connection
    conn.commit()
    conn.close()

