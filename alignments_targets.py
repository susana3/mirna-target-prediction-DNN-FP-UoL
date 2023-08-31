import gc
from Bio import pairwise2
from Bio.Seq import Seq
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')


def align_targets(genes):
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

    # Store the hsa and ath target gene transcripts with homology >= 60%.
    matching_targets = {}

    # Performs the alignment.
    try:
        best_alignment_score = pairwise2.align.globalms(hsa_gene_seq, ath_gene_seq,
                                                        1, -2,
                                                        -.5, -.2,
                                                        score_only=True)

        # Expresses the score as a percentage based on the shortest sequence.
        shortest_len = min(hsa_gene_len, ath_gene_len)
        best_alignment_perc = best_alignment_score / shortest_len
        # Sets the threshold to select or discard the sequence as valid homologous.
        threshold = 0.6
        if best_alignment_perc >= threshold:
            if hsa_gene_name not in matching_targets.keys():
                matching_targets[hsa_gene_name] = [(ath_gene_name, ath_gene_seq)]
            else:
                matching_targets[hsa_gene_name].append((ath_gene_name, ath_gene_seq))

        return matching_targets

    except MemoryError:
        print(f'Memory err: hsa-{hsa_gene_name}\tath-{ath_gene_name}')
        gc.collect()
