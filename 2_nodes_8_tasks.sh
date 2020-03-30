#!/bin/bash
#SBATCH -p physical
#SBATCH --nodes=2
#SBATCH --ntasks=8
#SBATCH --time=00:30:00
#SBATCH --job-name=T3
#SBATCH -e output/error/job3_error.txt
#SBATCH -o output/result/job3_result.txt

echo "Job_3_2_nodes_8_cores"
echo "------------------------------------"
module load Python/3.4.3-goolf-2015a
mpirun python main_final.py
