import gc
from Bio import pairwise2
from Bio.Seq import Seq
from time import perf_counter
import redis
import warnings
warnings.filterwarnings('ignore')


# Initialize redis database.
r = redis.Redis(host='localhost', port=6379, decode_responses=True, db=1)


# ---------- Helper functions to generate the redis keys to store the alignment results.
def r_pair(gene_name_1, gene_name_2):
    return f'{gene_name_1}_{gene_name_2}'


def r_pair_alignment_time(gene_name_1, gene_name_2):
    return f'{r_pair(gene_name_1, gene_name_2)}_time'


def r_pair_alignment_score(gene_name_1, gene_name_2):
    return f'{r_pair(gene_name_1, gene_name_2)}_score'


def r_pair_alignment_sequences(gene_name_1, gene_name_2):
    return f'{r_pair(gene_name_1, gene_name_2)}_sequences'


# ----------- Alignment.
def align_targets(genes):
    # Start timer.
    t1 = perf_counter()
    # Prepares the human gene.
    hsa_gene = genes[0]
    hsa_gene_name = hsa_gene[0]
    hsa_gene_seq = Seq(hsa_gene[1])
    hsa_gene_len = len(hsa_gene_seq)
    # Prepares the A. thaliana gene.
    ath_gene = genes[1]
    ath_gene_name = ath_gene[0]
    ath_gene_seq = Seq(ath_gene[1])
    ath_gene_len = len(ath_gene_seq)

    # Ensures the code only runs given that the pair is not already stored in the local database.
    if r.get(r_pair_alignment_time(hsa_gene_name, ath_gene_name)):
        return

    # Performs the alignment.
    try:
        best_alignment_score = pairwise2.align.globalms(hsa_gene_seq, ath_gene_seq,
                                                        1, -1,
                                                        -.3, -.1,
                                                        score_only=True)

        # Expresses the score as a percentage based on the shortest sequence.
        shortest_len = min(hsa_gene_len, ath_gene_len)
        best_alignment_perc = best_alignment_score / shortest_len
        # Sets the threshold to select or discard the sequence as valid homologous.
        threshold = 0.65
        if best_alignment_perc >= threshold:
            # Only stores identified homologous pairs.
            r.set(r_pair_alignment_score(hsa_gene_name, ath_gene_name),
                  best_alignment_perc)
            r.set(r_pair_alignment_sequences(hsa_gene_name, ath_gene_name),
                  ' '.join([hsa_gene[1], ath_gene[1]]))

        # Execution time is always stored if the alignment was successful, regardless the score result.
        t2 = perf_counter()
        execution_time = t2 - t1
        r.set(r_pair_alignment_time(hsa_gene_name, ath_gene_name), execution_time)
    except MemoryError:
        print(f'Memory err: hsa-{hsa_gene_name}\tath-{ath_gene_name}')
        gc.collect()
