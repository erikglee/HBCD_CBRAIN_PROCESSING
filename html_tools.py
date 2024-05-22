import requests, json, getpass, os
from datetime import date
import numpy as np
import boto3
from botocore.exceptions import ClientError
import pathlib
from pathlib import Path
import glob
import logging
import inspect
import pandas as pd
import botocore
import datetime 
import re
import base64
from io import BytesIO
import matplotlib.pyplot as plt
import html_tools

def color_specific_value_cells(styler, df, primary_column, value="specific_value", color="red", secondary_column=None):
    """
    Applies color to cells in the secondary column of a Styler object based on conditions met in the primary column
    of the provided DataFrame. If no secondary column is specified, applies coloring to the primary column.

    Parameters:
    - styler: pandas.io.formats.style.Styler, the Styler object of the DataFrame to apply styles to.
    - df: pandas.DataFrame, the DataFrame used to determine the styling conditions.
    - primary_column: str, the name of the primary column to check the condition against.
    - value: str, the value to check for in the primary column.
    - color: str, the CSS color to apply to cells in the secondary column based on the primary column's condition.
    - secondary_column: str, optional, the name of a secondary column where the color is applied based on the primary column's condition.

    Returns:
    - pandas.io.formats.style.Styler, the updated Styler object with applied styles.
    """
    # Create a mask based on the condition in the primary column
    mask = df[primary_column] == value

    # Define the function to apply styles
    def apply_styles(row):
        if row.name in df.index:
            primary_condition = mask.iloc[row.name]
            colors = [''] * len(row)  # Initialize with no coloring
            if primary_condition:
                if secondary_column:  # Apply color to the secondary column based on primary condition
                    colors[df.columns.get_loc(secondary_column)] = f'background-color: {color}'
                else:  # Or apply to the primary column if no secondary is specified
                    colors[df.columns.get_loc(primary_column)] = f'background-color: {color}'
        return colors

    # Apply the function across the DataFrame, row-wise
    updated_styler = styler.apply(apply_styles, axis=1)
    
    return updated_styler


def reformat_df_and_produce_proc_html(study_tracking_df, pipeline_name, output_html_path, file_selection_dict):
    
    study_tracking_df = study_tracking_df.sort_values(by=['subject', 'session'])
    study_tracking_df.reset_index(drop=True, inplace=True)
    
    study_tracking_df.fillna('Not Evaluated', inplace=True)
    study_tracking_df.loc[study_tracking_df["scans_tsv_present"] == 1, "scans_tsv_present"] = 'True'
    study_tracking_df.loc[study_tracking_df["scans_tsv_present"] == 0, "scans_tsv_present"] = 'False'
    study_tracking_df.loc[study_tracking_df["derivatives_found"] == 1, "derivatives_found"] = 'True'
    study_tracking_df.loc[study_tracking_df["derivatives_found"] == 0, "derivatives_found"] = 'False'
    styler = study_tracking_df.style
    for temp_key in file_selection_dict.keys():
        styler = color_specific_value_cells(styler, study_tracking_df, temp_key, 'Satisfied', color='Green')
        styler = color_specific_value_cells(styler, study_tracking_df, temp_key, 'Failed QC', color='Red')
        styler = color_specific_value_cells(styler, study_tracking_df, temp_key, 'No File', color='Yellow')
        styler = color_specific_value_cells(styler, study_tracking_df, temp_key, 'Already Processed', color='Green')
        styler = color_specific_value_cells(styler, study_tracking_df, temp_key, 'Missing QC', color='Yellow')
        


    for temp_col in study_tracking_df.columns:
        styler = color_specific_value_cells(styler, study_tracking_df, temp_col, 'Not Evaluated', color='Grey')
        if temp_col.startswith('CBRAIN_'):
            styler = color_specific_value_cells(styler, study_tracking_df, temp_col, 'Satisfied', color='Green')
            styler = color_specific_value_cells(styler, study_tracking_df, temp_col, 'No File', color='Red')


    styler = color_specific_value_cells(styler, study_tracking_df, 'scans_tsv_present', 'True', color='Green')
    styler = color_specific_value_cells(styler, study_tracking_df, 'scans_tsv_present', 'False', color='Red')

    red_cbrain_statuses = ['Terminated', 'Failed To Setup', 'Failed To PostProcess', 'Failed Setup Prerequisites', 'Failed PostProcess Prerequisites', 'Suspended', 'Failed', 'Failed On Cluster']
    for temp_status in red_cbrain_statuses:
        styler = color_specific_value_cells(styler, study_tracking_df, 'CBRAIN_Status', temp_status, color='Red')
        styler = color_specific_value_cells(styler, study_tracking_df, 'CBRAIN_Status', temp_status, color='Red', secondary_column = 'subject')
    styler = color_specific_value_cells(styler, study_tracking_df, 'CBRAIN_Status', 'Completed', color='Green')
    styler = color_specific_value_cells(styler, study_tracking_df, 'derivatives_found', 'True', color='Green', secondary_column = 'subject')
    styler = color_specific_value_cells(styler, study_tracking_df, 'derivatives_found', 'True', color='Green', secondary_column = 'CBRAIN_Status')
    styler = color_specific_value_cells(styler, study_tracking_df, 'derivatives_found', 'True', color='Green')



    html = styler.to_html(index = False)
    title = "Summary for HBCD {} processing".format(pipeline_name)
    text_list = []
    text_list.append("The color coding for the subject label is a quick presentation of a subject's processing. Green indicates processing has been completed. Yellow indicates that processing wasn't attempted because some prerequisite file was missing. Red indicates failed attempts at processing.")
    text_list.append("It is important to remember that a failed processing task can be due to a variety of reasons including actual issues with how the pipeline is interacting with the imaging data, or data-agnostic issues within CBRAIN or MSI.")
    text_list.append('"Not Evaluated" entries occur when we identify soemthing in the processing routine that prevents processing from being attempted. This includes (1) previous attempts at processing, (2) missing scans.tsv file, (3) failure to have reqiured BIDS files of sufficient quality in one or more categories of imaging modalities, (4) missing prerequisite outputs from another pipeline.')
    text_list.append('Any columns beginning with "CBRAIN_" represent file collections in CBRAIN that are expected for the current processing pipeline. These columns generally represent the outputs from previous processing pipelines that will be fed to the current pipeline for processing. If a value of "No File" is observed, this either means processing with the other pipeline was not attempted or unsuccessful.')
    text_list.append('There are certain cases where a subject has specific file types that are missing or failing QC but processing still occurred for the subject. This is because there are multiple sets of criteria that are used to determine whether a subject should be processed. For example if a subject has a good T1w and T2w image then both will be included for processing, but processing will still occur if only a good T2w image is available for processing. For a subject to be processed, they must have all the files needed to satisfy at least one of the requirements files for the given pipeline defined at https://github.com/erikglee/HBCD_CBRAIN_PROCESSING/tree/qc_aware_proc/comprehensive_processing_prerequisites . If at least one requirement is satisfied then as many files as possible (up to the limits defined by the "num_to_keep" field) will be included in processing.')
    html = add_title_and_list_to_html_content(html, title, text_list)
    
    #Create dict with instructions for pie charts
    plot_dict = {}
    columns_to_skip_plotting = ['subject', 'pipeline', 'session']
    for temp_col in study_tracking_df.columns:
        if temp_col not in columns_to_skip_plotting:
            plot_dict[temp_col] = study_tracking_df[temp_col].value_counts().to_dict()
    
    #Add pie charts to html
    html = append_pie_charts_to_html(html, plot_dict)
    
    
    html = add_prettier_background_to_html(html)
    with open(output_html_path, 'w') as f:
        f.write(html)
        
    return study_tracking_df


def append_pie_charts_to_html(html_str, data_dict):
    """
    Appends high-resolution pie charts to the end of an HTML string based on a provided dictionary.
    Each chart has a title, and the legend shows the count for each category.

    Parameters:
    - html_str: A string containing HTML.
    - data_dict: A dictionary with titles of plots and data for the pie charts.

    Returns:
    - A modified HTML string with pie charts appended.
    """

    # Start the section for pie charts in the HTML
    new_html = html_str + "\n<div id='pie-charts'>\n"

    for title, data in data_dict.items():
        # Prepare data for the pie chart
        labels = list(data.keys())
        sizes = list(data.values())
        # Create labels with counts for the legend
        legend_labels = [f"{label} ({size})" for label, size in zip(labels, sizes)]

        # Create a pie chart
        fig, ax = plt.subplots()
        wedges, texts = ax.pie(sizes, startangle=90, counterclock=False)
        ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
        plt.title(title)
        ax.legend(wedges, legend_labels, title="Categories", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))

        # Convert the plot to a PNG image, then encode it in base64 at high resolution
        img_data = BytesIO()
        plt.savefig(img_data, format='png', dpi=150, bbox_inches="tight")
        plt.close()  # Close the plot to free memory
        img_data.seek(0)  # Go to the beginning of the BytesIO buffer
        img_base64 = base64.b64encode(img_data.read()).decode('utf-8')

        # Embed the base64-encoded PNG image into the HTML
        img_html = f"<img src='data:image/png;base64,{img_base64}' alt='{title}'><br>\n"
        new_html += img_html

    # Close the pie charts section
    new_html += "</div>\n"

    return new_html

def add_prettier_background_to_html(html_content):
    """
    Modifies the given HTML content to include prettier background formatting using CSS.

    Parameters:
    - html_content: str, the HTML content as a string.

    Returns:
    - str, the modified HTML content with additional CSS for prettier formatting.
    """
    # Define the CSS for prettier background and overall formatting
    css_styles = """
    <style>
    body {
        font-family: Arial, sans-serif;
        background-color: #f0f0f0;
        margin: 20px;
        padding: 20px;
    }
    table {
        border-collapse: collapse;
        width: 100%;
    }
    th, td {
        text-align: left;
        padding: 8px;
    }
    tr:nth-child(even) {
        background-color: #dddddd;
    }
    th {
        background-color: #4CAF50;
        color: white;
    }
    h1 {
        color: #333;
    }
    ul {
        list-style-type: square;
    }
    li {
        margin-bottom: 5px;
    }
    </style>
    """

    # Insert the CSS at the beginning of the HTML content
    html_with_css = css_styles + html_content

    return html_with_css


def add_title_and_list_to_html_content(html_content, title, list_of_strings):
    """
    Modifies HTML content to include a title and a series of bullet points.

    Parameters:
    - html_content: str, the HTML content as a string.
    - title: str, the title to be added to the HTML.
    - list_of_strings: list, a list of strings to be converted into bullet points.

    Returns:
    - str, the modified HTML content with added title and bullet points.
    """
    # Create the title and bullet points HTML
    title_html = f'<h1>{title}</h1>\n'
    list_html = '<ul>\n' + ''.join([f'<li>{item}</li>\n' for item in list_of_strings]) + '</ul>\n'
    
    # Insert the title and list HTML before the table
    # Assuming the first occurrence of <table relates to the DataFrame's table
    table_index = html_content.find('<table')
    if table_index == -1:
        # If no table is found, prepend the title and list to the HTML content
        modified_html = title_html + list_html + html_content
    else:
        modified_html = html_content[:table_index] + title_html + list_html + html_content[table_index:]
    
    return modified_html