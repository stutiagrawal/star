import os
import time
import runBashCmd
import logging
import setupLog
import argparse
import subprocess

#Get path to compressed fastqs
def get_path_tarball(input_dir, analysis_id):
    filepath = ""
    data_dir = os.path.join(input_dir, analysis_id)
    if os.path.isdir(data_dir):
        for filename in os.listdir(data_dir):
            if (filename.endswith("tar.gz") or filename.endswith("tar")):
                filepath = os.path.join(data_dir, filename)
    return filepath

#Run the command
def run_command(cmd, logger):
    stdoutdata, stderrdata = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                              stderr=subprocess.PIPE).communicate()
    #Log the standard out
    stdoutdata = stdoutdata.split("\n")
    for line in stdoutdata:
        logger.info(line)
    stderrdata = stderrdata.split("\n")
    for line in stderrdata:
        logger.info(line)
    logger.info('Completed: %s' %cmd)

#Download the fastq files
def download_data(analysis_id, input_dir, cghub_key, logger):
    print "%s" %os.path.join(input_dir, analysis_id)
    if not os.path.isdir(os.path.join(input_dir, analysis_id)):
        print "Downloading data"
        cmd = ['gtdownload', '-v', '-c', cghub_key, '-p', input_dir, analysis_id]
        run_command(cmd, logger)
        print "Download complete"
        #os.system("gtdownload -v -c %s -p %s %s" %(cghub_key, input_dir, analysis_id))

#Upload data to swift
def upload_to_swift(analysis_id, input_dir, container, logger):
    filepath = get_path_tarball(input_dir, analysis_id)
    swift_file_size_cut_off = 5*1024**3 #5Gib in bits
    if not filepath == "":

        if os.path.getsize(filepath) < swift_file_size_cut_off:
            cmd = ['swift', 'upload', container, filepath]
        else:
            segment_size = 1024**3 #1GiB segments
            cmd = ['time', '/usr/bin/time', 'swift', 'upload',
                    container, filepath, '--segment-size=%s'%segment_size, '--verbose']
        print "Starting upload"
        run_command(cmd, logger)
        print "Upload Complete"

def download_from_swift_helper(cmd, logger):
    start_time = time.time()
    run_command(cmd)
    end_time = time.time()
    return (end_time - start_time)/60.0

def get_md5_sum(filename):
    """ md5 sum a file """

    md5worker = hashlib.md5()
    tar_file = open(filename)
    block_size = 128
    while True:
        data = tar_file.read(block_size)
        if not data:
            break
        md5worker.update(data)
    return md5worker.hexdigest()

def download_from_swift(analysis_id, filename, container,logger,md5_sum,
                        prefix='/mnt/cinder/tmp/data/rna_seq_datasets/'):
    """ Download from swift and check md5sum """

    filename = os.path.join(prefix, analysis_id, filename)
    cmd = ['time', '/usr/bin/time', 'swift', 'download', '--verbose', container, filename]
    num_tries = 0
    if not os.path.isfile(filename):
        time_taken = download_from_swift_helper(cmd, logger)
    while(get_md5_sum(filename) != md5_sum[filename]):
        num_tries += 1
        time_taken = download_from_swift_helper(cmd, logger)
        if num_tries > 5:
            logger.info("Failed proper download for %s. Please check the upload again")
    if num_tries < 5:
        logger.info("SWIFT_DOWNLOAD:\t%s\t%s" %(filename, time_taken)

def run_pipeline(analysis_id, input_dir, star_pipeline, genome_dir, logger):
    """ execute the STAR pipeline """

    print "Running pipeline"
    print analysis_id
    data_dir = os.path.join(input_dir, analysis_id)
    if os.path.isdir(data_dir):
        for filename in os.listdir(data_dir):
            if (filename.endswith("tar.gz") or filename.endswith("tar")):
                filepath = os.path.join(data_dir, filename)
                break
        output_bam = "%s.bam" %os.path.join(data_dir, analysis_id)
        if not os.path.isfile(output_bam):
            cmd = ['time', '/usr/bin/time', 'python', '%s' %star_pipeline, '--genomeDir', '%s' %genome_dir,
                    '--tarFileIn','%s' %filepath, '--workDir', '%s'%data_dir, '--out', '%s' %output_bam,
                    '--runThreadN', '12']
            start_time = time.time()
            run_command(cmd, logger)
            end_time = time.time()
            logger.info("STAR_ALIGN:\t%s\t%s\t%s" %(filename, float(os.path.getsize(filepath))/(1024**3),
                        float(end_time - start_time)/(60.0)))
    else:
        logger.error("Invalid path: %s" %(data_dir))
    #os.system("time /usr/bin/time python %s --genomeDir %s --tarFileIn %s --workDir %s --out %s --runThreadN 12"
    #        %(star_pipeline, genome_dir, filepath, data_dir, output_bam))
"""

if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog='test_star_align.py', description='Align using STAR')
    parser.add_argument('--input_dir', help='path to input dir', default = '/mnt/cinder/tmp/data/rna_seq_datasets')
    parser.add_argument('--star_pipeline', help='path to pipeline',
            default = '/mnt/cinder/tmp/data/icgc_rnaseq_align/star_align.py')
    parser.add_argument('--analysis_id_file', help = 'file for analysis ids',
            default = '/mnt/cinder/tmp/data/star_pipeline/data_set_1.txt')
    parser.add_argument('--genome_dir', help = 'path to genome index',
            default = '/mnt/cinder/tmp/data/rna_seq_star_genome_dir')
    parser.add_argument('--cghub_key', default = '/home/ubuntu/keys/cghub.key')
    parser.add_argument('--swift_container', default = 'rna_seq_datasets')
    args = parser.parse_args()


    fp = open(args.analysis_id_file, "r")
    for analysis_id in fp:
        analysis_id = analysis_id.rstrip()
        #if not (os.path.isdir(os.path.join(args.input_dir, analysis_id))):
        log_file = "%s.log" %(os.path.join(args.input_dir, analysis_id))
        logger = setupLog.setup_logging(logging.INFO, 'aligner', log_file)

        download_data(analysis_id, args.input_dir, args.cghub_key, logger)
        upload_to_swift(analysis_id, args.input_dir, args.swift_container, logger)

        #if(os.path.isdir(os.path.join(args.input_dir, analysis_id))):
        #    log_file = "%s.log" %(os.path.join(args.input_dir, analysis_id, analysis_id))
        #    logger = setupLog.setup_logging(logging.INFO, 'aligner', log_file)

        #    run_pipeline(analysis_id, args.input_dir, args.star_pipeline, args.genome_dir, logger)
