import os
import pipelineUtil
import argparse
import setupLog
import logging

def run_pipeline(args, workdir, analysis_id, logger):
    """ align datasets using STAR """

    for filename in os.listdir(workdir):
        if filename.endswith(".tar") or filename.endswith(".tar.gz"):
            tar_file_in = os.path.join(workdir, filename)
            break

    star_output_dir = os.path.join(workdir, 'star_2_pass')
    if os.path.isdir(star_output_dir):
        pipelineUtil.remove_dir(star_output_dir)
    os.mkdir(star_output_dir)
    bam = "%s_star.bam" %os.path.join(star_output_dir, analysis_id)

    if not os.path.isfile(bam):
        star_cmd = ['time', '/usr/bin/time', 'python', args.star_pipeline,
                    '--genomeDir', args.genome_dir,
                    '--runThreadN', args.p,
                    '--tarFileIn', tar_file_in,
                    '--workDir', workdir,
                    '--out', bam,
                    '--genomeFastaFile', args.genome_fasta_file,
                    '--sjdbGTFfile', args.gtf
                   ]
        if args.quantMode != "":
            star_cmd.append('--quantMode')
            star_cmd.append(args.quantMode)

    pipelineUtil.log_function_time("STAR", analysis_id, star_cmd, logger)

    remote_bam_path = "%s_star.bam" % os.path.join(args.bucket, analysis_id, analysis_id)
    pipelineUtil.upload_to_cleversafe(logger, remote_bam_path, bam)

    pipelineUtil.remove_dir(star_output_dir)


def download_missing_reference(args_input, remote_default, bucket):

    path = os.path.dirname(args_input)
    if not os.path.isdir(path):
        os.mkdir(path)
    pipelineUtil.download_from_cleversafe(None, os.path.join(bucket, remote_default), args_input)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog='pipeline.py', description='STAR')
    parser.add_argument('--analysis_id', required=True, default=None, type=str, help='analysis ids')
    parser.add_argument('--gtf', required=True, type=str, help='genome annotation file')
    parser.add_argument('--bucket', required=True, type=str, help='path to remote bucket')
    parser.add_argument('--p', type=str, default=1, help='number of threads')

    star = parser.add_argument_group("star pipeline")
    star.add_argument('--genome_dir', default='/home/ubuntu/SCRATCH/star_genome/', help='star index directory')
    star.add_argument('--star_pipeline', default='/home/ubuntu/icgc_rnaseq_align/star_align.py',
                      help='path to star pipeline')
    star.add_argument('--input_dir', default='/home/ubuntu/SCRATCH', help='parent path for all datasets')
    star.add_argument('--genome_fasta_file', type=str, help='path to reference genome',
                default='/home/ubuntu/SCRATCH/grch38_genome/GCA_000001405.15_GRCh38_no_alt_with_d1_chrUn.fa')
    star.add_argument('--quantMode', type=str, default="", help='enable transcriptome mapping in STAR')

    args = parser.parse_args()

    analysis_id = args.analysis_id

    workdir = os.path.join(args.input_dir, analysis_id)

    if not os.path.isdir(workdir):
        pipelineUtil.download_from_cleversafe(None, os.path.join(args.bucket, analysis_id), args.input_dir)

    if not os.path.isdir(args.genome_dir):
        pipelineUtil.download_from_cleversafe(None, os.path.join(args.bucket, 'star_genome'), args.input_dir)

    if not os.path.isfile(args.genome_fasta_file):
        default = "GCA_000001405.15_GRCh38_no_alt_with_d1_chrUn.fa"
        download_missing_reference(args.genome_fasta_file, default, args.bucket)

    if not os.path.isfile(args.gtf):
        default = "gencode.v22.annotation.gtf"
        download_missing_reference(args.gtf, default, args.bucket)

    if os.path.isdir(workdir):
        star_log_file = "%s_star.log" %(os.path.join(args.input_dir, analysis_id, analysis_id))
        logger = setupLog.setup_logging(logging.INFO, analysis_id, star_log_file)
        run_pipeline(args, workdir, analysis_id, logger)
        star_log_out = os.path.join(args.bucket, 'logs', '%s.log' %analysis_id)
        pipelineUtil.upload_to_cleversafe(logger,star_log_out, star_log_file)
        pipelineUtil.remove_dir(workdir)
