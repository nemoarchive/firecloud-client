#!/usr/bin/env python

import argparse
import arrow
import csv
import executor
import hashlib
import os
import shutil
from six.moves import urllib
import sys
import tarfile

# This function checks a file's MD5 checksum against a candidate hash
# Arguments:
# file_path = location of the file just downloaded which requires the integrity
# check
# original_md5 = verification MD5 hash
def checksum_matches(file_path, original_md5):
    md5 = hashlib.md5()

    # Read the file in chunks and build a final MD5
    with open(file_path,'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5.update(chunk)

    if md5.hexdigest() == original_md5:
        return True
    else:
        return False

# Function to retrieve a particular set of bytes from the file.
# Arguments:
# res = network object created by get_url_obj()
# endpoint = HTTP
# block_sz = number of bytes to be considered a chunk to allow interrupts/resumes
# start_pos = position to start at
# max_range = maximum value to use for the range, same as the file's size
# file = file handle to write out to
def get_buffer(res, endpoint, block_size, start_pos, max_range, file):
    return res.read(block_size)

# Function to retrieve the file size.
# Arguments:
# url = path to location of file on the web
def get_file_size(url):
    return int(urllib.request.urlopen(url).info()['Content-Length'])

# Function to output a status message to the user.
# Argument:
# message = the string to temporarily output to the user
def generate_status_message(message):
    status = message
    status = status + chr(8) * (len(status) + 1) # backspace everything
    sys.stdout.write("\r{0}".format(status))

# Function to get the URL for the prioritized endpoint that the user requests.
# Note that priorities can be a list of ordered priorities.
# Arguments:
# manifest_urls = the CSV set of endpoint URLs
# priorities = priorities declared when calling client.py
def get_prioritized_endpoint(manifest_urls, priorities):
    url_list = []

    urls = manifest_urls.split(',')
    eps = priorities.split(',')

    # If the user didn't provide a set of priorities, then prioritize based on
    # whether on an EC2 instance.
    if eps[0] == "":
        eps = ['HTTP', 'S3'] # if none provided, use this order

    # Go through and build a list starting with the higher priorities first.
    for ep in eps:
        for url in urls:
            if url.startswith(ep.lower()):

                # Quick fix until the correct endpoints for the demo data (bucket+key) are established on S3.
                if 's3://' in url and 'HMDEMO' in url:
                    elements = url.split('/')
                    url = "s3://{0}/DEMO/{1}/{2}".format(elements[2],elements[4],"/".join(elements[-4:]))

                url_list.append(url)

    return url_list

def download_manifest(manifest, destination, bucket, no_verify=False, block_size=1000000):
    # build a list of elements to indicate how many and why the files failed
    # 1 = no valid URL in manifest
    # 2 = URL exists, but not accessible at the location specified
    # 3 = MD5 check failed for file (file is corrupted or the wrong MD5 is attached to the file)
    failed_files = []

    file_groups = []

    # iterate over the manifest data structure, one ID/file at a time
    for mfile in manifest:
        url_list = get_prioritized_endpoint(mfile['urls'], "")

        # Handle private data or simply nodes that are not correct and lack
        # endpoint data
        if not url_list:
            print("No valid URL found in the manifest for file ID {0}".format(mfile['id']))
            failed_files.append(1)
            continue

        file_name = "{0}/fastqs/{1}".format(destination, url_list[0].split('/')[-1])

        # only need to download if the file is not present
        if os.path.exists(file_name):
            print("File already exists.")
            continue

        tmp_file_name = "{0}.partial".format(file_name)

        # If we only have part of a file, get the new start position
        current_byte = 0
        if os.path.exists(tmp_file_name):
            current_byte = os.path.getsize(tmp_file_name)

        # Need to try get the others to work like this, but for now HTTP
        # is the only one that can pull bytes in chunks without needing to
        # drop the connection.
        http_header = {}
        http_header['Range'] = 'bytes={0}-'.format(current_byte)

        res = None
        endpoint = ""
        eps = []

        for url in url_list:
            endpoint = url.split(':')[0].upper()
            if endpoint == "HTTPS":
                endpoint = "HTTP"
            eps.append(endpoint)

            try:
                res = get_url_obj(url, http_header)
                break
            except Exception as e:
                print(e)

        if res is None: # if all attempts resulted in no object, move on to next file
            print("Skipping file ID {0} as none of the URLs checked {1} yielded a valid file".format(mfile['id'], eps))
            failed_files.append(2)
            continue

        with open(tmp_file_name, 'ab') as file:

            # Need to pull the size without the potential bytes buffer
            file_size = get_file_size(url)
            print("Downloading file (via {0}): {1} | total bytes = {2}".format(endpoint, file_name, file_size))

            while True:
                if block_size > file_size:
                    generate_status_message("block size greater than total file size, pulling entire file in one go")

                buffer = get_buffer(res, endpoint, block_size, current_byte, file_size, file)

                if not buffer: # note that only HTTP/S3 make it beyond this point
                    break

                file.write(buffer)

                current_byte += len(buffer)
                generate_status_message("{0}  [{1}%]".format(current_byte, int(current_byte * 100 / file_size)))

            # Move the .partial file to the final name
            shutil.move(tmp_file_name, file_name)

            # Check the checksum (unless we've been told not to...
            if no_verify:
                print("Skiping checksum verification for file {}.".format(file_name))
            else:
                if checksum_matches(file_name, mfile['md5']):
                    print("Checksum verification passed for file {}.".format(file_name))
                else:
                    print("MD5 check failed for the file {}. Data may be corrupted.".format(file_name))
                    failed_files.append(3)

            print("Untarring downloaded file.")
            try:
                file_group = untar_files_downloaded(destination)
                file_groups.append(file_group)
                print("Downloaded file successfully extracted.")
            except Exception as e:
                sys.stderr.write("Errors encountered untarring data: {}.\n".format(e))
                clear_fastqs(destination)
                continue

            print("Uploading data to firecloud (Bucket {})".format(bucket))
            try:
                upload2bucket(destination, bucket, recursive=True)
            except Exception as e:
                sys.stderr.write("Error uploading the data to firecloud: {}.\n".format(e))
                continue

            clear_fastqs(destination)

    return file_groups

def clear_fastqs(destination):
    fastqs = os.path.join(destination, 'fastqs')

    for entry in os.listdir(fastqs):
        file_path = os.path.join(fastqs, entry)
        os.unlink(file_path)

# Function to get a network object of the file that can be iterated over.
# Arguments:
# url = path to location of file on the web
# http_header = HTTP range to pull from the file, the other endpoints require
# this processing in the get_buffer() function.
def get_url_obj(url, http_header):
    res = None

    try:
        req = urllib.request.Request(url)
        res = urllib.request.urlopen(req)
    except:
        raise Exception("Uh oh")

    return res

def get_timestamp():
    timestamp = arrow.utcnow()
    return str(timestamp)

def make_data_dir(output_dir, timestamp):
    dirname = '{}-{}'.format('upload', timestamp)
    fullpath = os.path.join(output_dir, dirname)
    fastqs_dir = os.path.join(fullpath, 'fastqs')
    os.mkdir(fullpath)
    os.mkdir(fastqs_dir)
    return fullpath

def upload2bucket(local_path, bucket_location, recursive=False):
    bucket_formal = bucket_location
    # Check if the bucket starts with gs://'. If it does not, then
    # we add it ourselves.
    if not bucket_location.startswith('gs://'):
        bucket_formal = 'gs://' + bucket_location

    command = ['gsutil', 'cp']

    # Check if we are in recursive mode. If so, use the -r option.
    if recursive:
        command.append('-r')

    command.extend([local_path, bucket_formal])

    (exit_code, stdout, stderr) = executor.run_command(command)

    if exit_code == 0:
        print('Successfully uploaded to GCP location {}.'.format(bucket_location))
    else:
        sys.stderr.write(stderr)
        raise Exception("Upload to GCP location {} failed.". format(bucket_location))

def untar_files_downloaded(download_dir):
    download_dir = os.path.join(download_dir, 'fastqs')

    entries = os.listdir(download_dir)

    # Container to hold the file membership of each tar
    collection = []

    for entry in entries:
        if entry.endswith('.tar'):
            files = untar_file(os.path.join(download_dir, entry))
            collection.append(files)
        else:
            print("Skipping file {}".format(entry))

    return collection

def untar_file(path):
    # What directory is the tar file in?
    # We need this top perform the exraction in the same directory
    tardir = os.path.dirname(path)

    try:
        tar = tarfile.open(path, "r")
        contents = tar.getmembers()
        tar.extractall(tardir)

        # Now delete the file
        os.unlink(path)
    except:
        sys.stderr.write("Problem untarring file {}\n".format(path))

    return contents

def create_descriptor(file_groups, directory, timestamp):
    plural = "sets"
    if len(file_groups) == 1:
        plural = "set"

    print("Processing {} {} of files.".format(len(file_groups), plural))

    sample_file_path = os.path.join(directory, 'sample-{}.txt'.format(timestamp))
    sample_fh = open(sample_file_path, 'w')

    for group_list in file_groups:
        # Each "group" of 10X files has three files in it: an R1 file, and R2 file, and an I1 file
        r1 = None
        r2 = None
        i1 = None

        for group in group_list:
            for tarfile in group:
                name = tarfile.name

                if name.endswith('.fastq.gz'):
                    if "_R1_" in name:
                        r1 = name
                    elif "_R2_" in name:
                        r2 = name
                    elif "_I1_" in name:
                        i1 = name
                else:
                    print("Skipping file {}".format(name))

            if r1 is not None and r2 is not None and i1 is not None:
                sample = r1.split('_R1_')[0]
                sample_line = '\t'.join([sample, r1, r2, i1]) + "\n"

                sample_fh.write(sample_line)
            else:
                names = []

                for tarfile in group:
                    name = tarfile.name

                names.append(name)
                sys.stderr.write("Group did not contain 1 each of R1, R2 and I1 files.\n")
                sys.stderr.write("\n".join(names) + "\n")
                continue

    sample_fh.close()

    return sample_file_path

def parse_manifest(manifest_path):
    manifest_fh = open(manifest_path, 'rb')
    reader = csv.reader(manifest_fh, delimiter="\t")

    # Skip the first row of the CSV file, which is just header information
    next(reader)

    # Format of the manifest (tsv) file is:
    # file_id md5 size urls sample_id
    manifest = []

    for row in reader:
        file_id = row[0]
        md5_checksum = row[1]
        size = row[2]
        urls_csv = row[3]
        sample_id = row[4]

        mfile = {
            'id': file_id,
            'md5': md5_checksum,
            'size': size,
            'sample_id': sample_id,
            'urls': urls_csv
        }

        manifest.append(mfile)

    # Don't need the manifest filehandle anymore. Close it.
    manifest_fh.close()

    return manifest

def parse_cli():
    parser = argparse.ArgumentParser(
        description='Transfer data from the NeMO archive to FireCloud.'
    )

    parser.add_argument('-m', '--manifest',
        type=str,
        required=True,
        help='Location of the local manifest file from portal.nemoarchive.org.')
    parser.add_argument('-d', '--directory',
        type=str,
        required=True,
        help='Directory to use for processing.')
    parser.add_argument('--no-verify',
        action='store_true',
        dest='no_verify',
        default=False,
        required=False,
        help='Skip checksum verification of downloaded files.')
    parser.add_argument('-b', '--bucket',
        type=str,
        required=True,
        help='Bucket ID on Google Cloud Platform (GCP). Specify without the gs:// prefix.')

    args = parser.parse_args()

    return args

def main():
    # Parse the command-line arguments
    args = parse_cli()

    manifest_path = args.manifest
    bucket = args.bucket
    topdir = args.directory
    no_verify = args.no_verify

    # We use timestamps in order to name things, so we need to obtain a timestamp
    # (in UTC format, naturally).
    timestamp = get_timestamp()
    download_dir = make_data_dir(topdir, timestamp)

    # Check if the manifest file exists before attempting to use it
    if not os.path.exists(manifest_path):
        sys.stderr.write('Manifest file does not exist at {}'.format(manifest_path))
        sys.exit(1)

    # Parse the contents of the manifest file into memory
    manifest = parse_manifest(manifest_path)

    # Download the data files from the NeMO Archive into the prepared
    # download directory, and for each one, upload it to the specified
    # GCP bucket.
    file_groups = download_manifest(manifest, download_dir, bucket, no_verify=no_verify)

    print("Creating sample descriptor for firecloud.")
    try:
        sample_descriptor = create_descriptor(file_groups, download_dir, timestamp)
    except Exception as e:
        sys.stderr.write("Error creating the firecloud descriptor: {}.\n".format(e))
        sys.exit(1)

    print("Uploading sample descriptor to firecloud")
    try:
        dest = "/".join([bucket, os.path.basename(download_dir)]) + "/"
        upload2bucket(sample_descriptor, dest)
    except Exception as e:
        sys.stderr.write("Error creating the firecloud descriptor: {}.\n".format(e))
        sys.exit(1)

    # Check for download failures
    print("Process completed.")

main()
