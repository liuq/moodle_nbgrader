import nbgrader, csv, codecs, sys, os, shutil
from nbgrader.apps import NbGraderAPI
import zipfile
import pandas as pd

import logging

logger = logging.getLogger('moodle_nbgrader')
logger.setLevel(logging.INFO)

def zip(out, root):
    shutil.make_archive(out, 'zip', root)

def add_feedback_to_zip(archive, unique_id, ident, fullname, assignment):
    fbk_path = os.path.join("feedback", str(unique_id), assignment)
    
    try:                                    
        files = [os.path.join(fbk_path, f) for f in os.listdir(fbk_path) if f.endswith('.html')]
        
        assign_id = ident.strip('Participant ')
        # remove asterisks
        name = 'blank'
        
        # create the path to the feedback file
        for f in files:
            archive.write(f, arcname=os.path.join(f"{fullname}_{assign_id}_assignsubmission_file_", os.path.basename(f)))
        
    except FileNotFoundError:
        logger.error(f"HTML feedback file for {fullname} {unique_id} {assignment} is missing")
        # no feedback to generate


def update_grade(out_df, index, unique_id, fullname, submission):
    out_df.loc[index, 'Grade'] = submission.score

    # warn about dubious scores
    if submission.score <= 0 or submission.score > submission.max_score:
        logger.warning(f"Warning: {unique_id} {fullname} has a score of {submission.score}")

    # correct the maximum grade
    out_df.loc[index, 'Maximum Grade'] = submission.max_score

def moodle_gradesheet(assignment, with_feedback=True):    
    
    api = NbGraderAPI()
    gradebook = api.gradebook        
    csvfile = os.path.join("imports", assignment + ".csv")
    grading_df = pd.read_csv(csvfile)
    
    fname =  os.path.join("exports", assignment + ".csv")
    
    if with_feedback:
        archive = zipfile.ZipFile(os.path.join("exports", "feedback_"+assignment+".zip"), 'w', zipfile.ZIP_DEFLATED)
        
    out_df = grading_df.copy()   
     # these are the students that are grouped
    grading_df['actual_group'] = grading_df['Group']
    individuals = (grading_df['Department'] == grading_df['Group']) | (grading_df['Group'] == 'Default group')
    grading_df.loc[individuals, 'actual_group'] = grading_df.loc[individuals, 'Identifier']   
    n_groups = len(grading_df['actual_group'].unique())

    for index, line in grading_df.drop_duplicates(subset='actual_group').iterrows():   
        if line['actual_group'] == 'Default Group':
            continue

        try:
            submission = gradebook.find_submission(assignment, line['ID number'])                
        except:
            if "Submitted" in line['Status']:
                logger.warning(f"No submission for {line['ID number']} in assignment {assignment}")
            else:
                logger.info(f"No submission for {line['ID number']} in assignment {assignment}, as expected")
        else:
            logger.info(f"Processing submission for {line['ID number']} in assignment {assignment}")

            # add feedback and grading to all students with the same submission in the same group

            for group_index, group_line in grading_df[grading_df['actual_group'] == line['actual_group']].iterrows():
                if with_feedback:
                    add_feedback_to_zip(archive, line['ID number'], group_line['Identifier'], group_line['Full name'], assignment)

                update_grade(out_df, group_index, group_line['ID number'], group_line['Full name'], submission)
            
    out_df.to_csv(fname, index=False)
    logger.info(f"Wrote to {fname}")

    # tidy up the feedback file
    if with_feedback:
        archive.close()
                    

import argparse

parser = argparse.ArgumentParser(description='''
    Updates a CSV file gradesheet (which must have be downloaded from
    Moodle with "offline gradesheets" enabled in the assignment settings) with
    the results from grading the assignment <assign>.

    The input will be imports/<assign>.csv
    The output will be in exports/<assign>.csv
            
    Feedback will be zipped up into the file exports/<assign>_feedback.zip and this
    can be uploaded to Moodle if "Feedback files" is enabled. This uploads all student
    feedback in one go.
''')

parser.add_argument('assignment', type=str, help='Name of the assignment csv file downloaded from moodle')

args = parser.parse_args()

moodle_gradesheet(args.assignment)
    