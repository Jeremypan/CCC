#!/bin/bash
#SBATCH -p physical
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --cpus-per-task=1
#SBATCH --time=00:30:00
#SBATCH --job-name=T2
#SBATCH -e output/error/job2_error.txt
#SBATCH -o output/result/job2_result.txt

echo "Job_2_1_node_8_cores"
echo "------------------------------------"
module load Python/3.4.3-goolf-2015a
mpirun python main_final.py