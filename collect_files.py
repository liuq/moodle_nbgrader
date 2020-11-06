import os
import re
import nbgrader, codecs, sys, os, shutil
import pandas as pd
from nbgrader.apps import NbGraderAPI
import zipfile  
import shutil
import logging

logger = logging.getLogger('moodle_nbgrader')
logger.setLevel(logging.INFO)

def moodle_gradesheet(notebook_name, assign_name, csvfile, zip):        

    api = NbGraderAPI()    
    gradebook = api.gradebook
    
    archive = zipfile.ZipFile(zip)
    fnames = {}

    # read all the filenames, and get the submission
    # ids for each filename
    for f in archive.filelist:
        fname = f.filename
        match = re.match(r"[\*\w\-\'\s\.,]+_([0-9]+)_.*", fname)
        if match:
            fnames[match.groups()[0]] = fname
        else:
            logger.error(f"Directory name {fname} did not match with regex")

    grading_df = pd.read_csv(csvfile)

    assign_matric = {} 
    n_rows = 0
    successful_files = 0
    missing_files = 0
    problem_files = 0

    # in order to take care of groups, first we dispatch the students then the files        
    for index, line in grading_df.iterrows():        
        
        ident, matric, fullname, group, department, email, status = line['Identifier'], \
            line['ID number'], line['Full name'], line['Group'], line['Department'], \
            line["Email address"], line['Status']

        # make sure we have this student in our records
        unique_id = str(matric)
        try:
            result = gradebook.find_student(unique_id)
        except nbgrader.api.MissingEntry:
            logging.info(f"Creating gradebook entry for {unique_id}")
            first_name = fullname.split(" ")[0]
            last_name = " ".join(fullname.split(" ")[1:])
            gradebook.update_or_create_student(unique_id, first_name=first_name, last_name=last_name, email=email)

    # now we dispatch the file, however one only for each group

    # these are the students that are grouped
    grading_df['actual_group'] = grading_df['Group']
    n_groups = len(grading_df['actual_group'].unique())

    for index, line in grading_df.drop_duplicates(subset='actual_group').iterrows():        
        
        ident, matric, fullname, group, department, email, status = line['Identifier'], \
            line['ID number'], line['Full name'], line['Group'], line['Department'], \
            line["Email address"], line['Status']

        if line['actual_group'] == 'Default Group':
            continue

        should_be_submission =  "Submitted" in status
        unique_id = str(matric)
            
        # map assignment numbers to matric numbers
        match = re.match('Participant ([0-9]+)', ident)
        if not match:
            logging.error(f"Could not find identity for participant {ident}")
            continue
        
        ident = match.groups()[0]
        assign_matric[ident] = unique_id
        
        n_rows += 1
        if ident in fnames:                
            # extract each file to the submission directory
            submission_path = os.path.join("submitted", unique_id, assign_name)
            try:
                os.makedirs(submission_path)
            except:
                pass
            fname = fnames[ident]
            notebook_file = os.path.basename(fname)
            notebook_file = notebook_name + ".ipynb"
            logging.info(f"Extracting {notebook_file} to {submission_path}")

            source = archive.open(fname)
            target = open(os.path.join(submission_path, notebook_file), "wb")
            with source, target:
                shutil.copyfileobj(source, target)
            
            successful_files += 1
        else:
            # submission was in the CSV file, but we don't have a zip file
            if should_be_submission:
                logging.warning(f"No submission for {fullname} {matric} but submission status was '{status}'")
                problem_files += 1
            else:
                # submission was not listed in the CSV file as being submitted
                logging.warning(f"No submission for {fullname} {matric} '{status}' as expected")
                missing_files +=1

    # print out a summary of what was processed
    print(
f"""{successful_files:d} succesfully extracted of {len(fnames):d} files in the ZIP archive.
{n_groups:d} groups/individuals.
{missing_files:d} files were not submitted, as expected.
{problem_files:d} files were missing, but showed as submitted on Moodle.
{successful_files + missing_files + problem_files:d} records were processed, and {n_rows} rows in the CSV.
"""
)            

import argparse

parser = argparse.ArgumentParser(description='''
    Collects notebook files into submitted and updates the gradebook DB. 
    It expects the files inside the `imports/` directory and the results will be copied 
    into submitted/<matric_id>/<submission_name>/<notebook_name>.ipynb
''')

parser.add_argument('assignment', type=str, help='Name of the assignment csv file downloaded from moodle')
parser.add_argument('notebook', type=str, help='Name of the notebook')

args = parser.parse_args()

moodle_gradesheet(args.notebook, args.assignment, os.path.join("imports", args.assignment + ".csv"), os.path.join("imports", args.assignment + ".zip"))
