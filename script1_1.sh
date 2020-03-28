#!/bin/bash
#SBATCH -p physical
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=00:05:00
#SBATCH --job-name=T1
#SBATCH -e output/error/job1_error.txt
#SBATCH -o output/result/job1_result.txt

echo "Job_1_1_node_1_cores"
echo "------------------------------------"
module load Python/3.4.3-goolf-2015a
mpiexec python main_final.py