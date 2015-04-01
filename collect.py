import os



def collect_metrics(root_dir):
    for analysis_id in os.listdir(root_dir):
        sub_dir = os.path.join(root_dir, analysis_id)
        if os.path.isdir(sub_dir):
            for log_file in os.listdir(sub_dir):
                if log_file.endswith("_star.log"):
                    log_file = os.path.join(sub_dir, log_file)
                    logp = open(log_file, "r")
                    for line in logp:
                        if "STAR_ALIGN" in line:
                            line = line.split()
                            print "%s\t%s\t%s\t%s" %(analysis_id, line[4], line[5], line[6])

if __name__ == "__main__":
    root_dir = "/home/ubuntu/SCRATCH"
    collect_metrics(root_dir)

