import os
import time
import runBashCmd
import logging
import setupLog
import argparse

#Download the fastq files
def download_data(analysis_id, input_dir, cghub_key):
    print "%s" %os.path.join(input_dir, analysis_id)
    if not os.path.isdir(os.path.join(input_dir, analysis_id)):
        print "Downloading data"
        os.system("gtdownload -v -c %s -p %s %s" %(cghub_key, input_dir, analysis_id))

def run_pipeline(analysis_id, input_dir, star_pipeline, genome_dir, logger):
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
                    '--runThreadN', '12', '--genomeFastaFile', '/home/ubuntu/SCRATCH/grch38/with_decoy/bowtie2_2/bowtie2_buildname.fa']
            start_time = time.time()
            runBashCmd._do_run(cmd, logger=logger)
            end_time = time.time()
            logger.info("STAR_ALIGN:\t%s\t%s\t%s" %(filename, float(os.path.getsize(filepath))/(1024**3),
                        float(end_time - start_time)/(60.0)))
    else:
        logger.error("Invalid path: %s" %(data_dir))
    """
    os.system("time /usr/bin/time python %s --genomeDir %s --tarFileIn %s --workDir %s --out %s --runThreadN 12"
            %(star_pipeline, genome_dir, filepath, data_dir, output_bam))
    """


if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog='test_star_align.py', description='Align using STAR')
    parser.add_argument('input_dir', help='path to input dir', default = '/mnt/cinder/tmp/data/rna_seq_datasets')
    parser.add_argument('star_pipeline', help='path to pipeline',
            default = '/mnt/cinder/tmp/data/icgc_rnaseq_align/star_align.py')
    parser.add_argument('analysis_id_file', help = 'file for analysis ids',
            default = '/mnt/cinder/tmp/data/star_pipeline/data_set_1.txt')
    parser.add_argument('genome_dir', help = 'path to genome index',
            default = '/mnt/cinder/tmp/data/rna_seq_star_genome_dir')
    parser.add_argument('cghub_key', default = '/home/ubuntu/keys/cghub.key')
    args = parser.parse_args()


    fp = open(args.analysis_id_file, "r")
    for analysis_id in fp:
        analysis_id = analysis_id.rstrip()
        if not (os.path.isdir(os.path.join(args.input_dir, analysis_id))):
            next
            #download_data(analysis_id, args.input_dir, args.cghub_key)
        if(os.path.isdir(os.path.join(args.input_dir, analysis_id))):
            log_file = "%s_star.log" %(os.path.join(args.input_dir, analysis_id, analysis_id))
            logger = setupLog.setup_logging(logging.INFO, analysis_id, log_file)
            run_pipeline(analysis_id, args.input_dir, args.star_pipeline, args.genome_dir, logger)
