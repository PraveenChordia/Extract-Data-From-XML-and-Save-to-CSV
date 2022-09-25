import logging
import os
import re
import xml.etree.ElementTree as ET
from zipfile import ZipFile

import boto3
import pandas as pd
import requests
import xmltodict

logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.addHandler(logging.FileHandler('Logs.txt'))
logger.setLevel(logging.DEBUG)


def download_xml_file(xmlfileurl):
    filename = 'xml-file-with-download-link.xml'
    try:
        # Downloading the file and saving it to temp storage
        xmlfiledata = requests.get(xmlfileurl)
        if xmlfiledata.status_code != 200:
            raise
    except Exception:
        logger.error('XML URL is wrong or faulty')
        return -1

    try:
        with open(filename, 'wb') as file:
            file.write(xmlfiledata.content)
        return filename
    except Exception:
        logger.error('Could not save XML File')
        return -1


def download_extract_zip_file(xml_file_name, file_type_to_process):
    xml_file_root = ET.parse(xml_file_name).getroot()

    zip_download_url = []
    # Above list will have all download links and their types available
    for child in xml_file_root.iter('doc'):
        templist = []
        for sub_child in child.iter():
            if sub_child.attrib == {'name': 'download_link'}:
                templist.append(sub_child.text)
            if sub_child.attrib == {'name': 'file_type'}:
                templist.append(sub_child.text)
        zip_download_url.append(templist)

    if len(zip_download_url) == 0:  # In Case There are no Download Links present
        logger.warning('No Download Url available')
        exit(-1)

    # Getting the first link with type reqired
    filetodownload = ''
    for i in zip_download_url:
        if i[1] == file_type_to_process:
            filetodownload = i[0]
            break
    if filetodownload == '':
        logger.warning('No Download Link of required type available')
        exit(-1)

    try:
        # Downloading the reqired file and saving
        downloadfile = requests.get(filetodownload)
        zipfilename = filetodownload.split('/')[-1]  # Using the file download URL to get file name.
    except Exception:
        logger.error('Zip Download Link not Valid')
        exit(-1)

    # Now we will Save the zip file and Extract XML File Containing desired data
    try:
        with open(zipfilename, 'wb') as output_file:
            output_file.write(downloadfile.content)

        # Extracting the zip file
        file = ZipFile(zipfilename)
        file.extractall()
        filetoprocessname = re.sub('zip', 'xml', zipfilename)  # Name of extracted xml file
        return filetoprocessname
    except:
        logger.error('Could not save and extract zip file')
        exit(-1)


def extract_data(filetoprocess):
    try:
        # Converting the XML file with required data to Dictionary Using xmltodict Library for easy working
        with open(filetoprocess, encoding='utf8') as xmlfile:
            dataxmlfile = xmltodict.parse(xmlfile.read())
    except Exception:
        logger.error('Could not open File to process data. FileName = ', filetoprocess)

    try:
        data = dataxmlfile['BizData']['Pyld']['Document']['FinInstrmRptgRefDataDltaRpt']['FinInstrm']
        # Above is list that contains the mixed data now we will extract the required data from it.
        data_extracted = []  # This list has the extracted data.
        for part in data:
            keys = part.keys()
            for key in keys:
                Id = part[key]['FinInstrmGnlAttrbts']['Id']
                FullNm = part[key]['FinInstrmGnlAttrbts']['FullNm']
                ClssfctnTp = part[key]['FinInstrmGnlAttrbts']['ClssfctnTp']
                NtnlCcy = part[key]['FinInstrmGnlAttrbts']['NtnlCcy']
                CmmdtyDerivInd = part[key]['FinInstrmGnlAttrbts']['CmmdtyDerivInd']
                Issr = part[key]['Issr']
                data_extracted.append([Id, FullNm, ClssfctnTp, NtnlCcy, CmmdtyDerivInd, Issr])
        return data_extracted
    except Exception:
        logger.error('Problem in Extracting relevent Data from: ', filetoprocess)
        exit(-1)


def create_csv_file(data_extracted, filetoprocess):
    columns = [
        'FinInstrmGnlAttrbts.Id',
        'FinInstrmGnlAttrbts.FullNm',
        'FinInstrmGnlAttrbts.ClssfctnTp',
        'FinInstrmGnlAttrbts.CmmdtyDerivInd',
        'FinInstrmGnlAttrbts.NtnlCcy',
        'Issr']
    try:
        # Converting the Data extracted to pandas dataframe
        data_frame = pd.DataFrame(data_extracted, columns=columns)
        csvfilename = re.sub('xml', 'csv', filetoprocess)
        data_frame.to_csv(csvfilename)  # Saving Data to CSV File
        return csvfilename
    except Exception:
        logger.error('Could not create CSV file.')
        exit(-1)


def upload_file_to_s3(csvfilename):
    # Here we upload Data to AWS S3
    try:
        s3 = boto3.resource('s3')
        bucket = 'steeleyeassignmentbucket'
        data = open(csvfilename, 'rb')
        s3.Bucket(bucket).put_object(Key=csvfilename, Body=data)
        logger.info('Upload Complete')
    except Exception:
        logger.error('Error in uploading File to S3')
        exit(-1)


if __name__ == '__main__':
    logger.info('Starting Process')
    os.chdir('./tmp')
    logger.info('Changed Directory to tmp')

    # File type which we have to process data from
    file_type_to_process = 'DLTINS'
    # URL of XML File with download link to zip file
    xml_file_url = 'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select' \
                   '?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100'

    # Download the XML File with Download Links
    xml_file_name = download_xml_file(xml_file_url)
    logger.info('Downloaded XML File: ' + xml_file_name)

    # Parsing the xml file to get download links to zip files and extract the zip file
    filetoprocess = download_extract_zip_file(xml_file_name, file_type_to_process)
    logger.info('Downloaded and Extracted the Zip File Containing the Data: ' + filetoprocess)

    # Now we will Extract The Data
    data_extracted = extract_data(filetoprocess)
    logger.info('Data Extracted')

    # Saving Extracted data to CSV File
    csvfilename = create_csv_file(data_extracted, filetoprocess)
    logger.info('Created CSV File: ' + csvfilename + ', Ready for uploading to S3')

    # Uploading CSV File to S3
    upload_file_to_s3(csvfilename)
    logger.info('File Uploaded to S3')
    logger.info('****Finished Processing****')
