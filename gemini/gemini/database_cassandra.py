#!/usr/bin/env python

import sys
from itertools import repeat
import contextlib

from ped import get_ped_fields, default_ped_fields
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel


def index_variation(session):
    session.execute('''create index var_chr_start_idx on\
                      variants(chrom, start)''')
    session.execute('''create index var_type_idx on variants(type)''')
    session.execute('''create index var_gt_counts_idx on \
                      variants(num_hom_ref, num_het, \
                               num_hom_alt, num_unknown)''')
    session.execute('''create index var_aaf_idx on variants(aaf)''')
    session.execute('''create index var_in_dbsnp_idx on variants(in_dbsnp)''')
    session.execute('''create index var_in_call_rate_idx on variants(call_rate)''')
    session.execute('''create index var_exonic_idx on variants(is_exonic)''')
    session.execute('''create index var_coding_idx on variants(is_coding)''')
    session.execute('''create index var_lof_idx on variants(is_lof)''')
    session.execute('''create index var_som_idx on variants(is_somatic)''')
    session.execute('''create index var_depth_idx on variants(depth)''')
    session.execute('''create index var_gene_idx on variants(gene)''')
    session.execute('''create index var_trans_idx on variants(transcript)''')
    session.execute('''create index var_impact_idx on variants(impact)''')
    session.execute('''create index var_impact_severity_idx on variants(impact_severity)''')
    session.execute('''create index var_esp_idx on variants(aaf_esp_all)''')
    session.execute('''create index var_1kg_idx on variants(aaf_1kg_all)''')
    session.execute('''create index var_qual_idx on variants(qual)''')
    session.execute('''create index var_homref_idx on variants(num_hom_ref)''')
    session.execute('''create index var_homalt_idx on variants(num_hom_alt)''')
    session.execute('''create index var_het_idx on variants(num_het)''')
    session.execute('''create index var_unk_idx on variants(num_unknown)''')
    session.execute('''create index var_omim_idx on variants(in_omim)''')
    session.execute('''create index var_cadd_raw_idx on variants(cadd_raw)''')
    session.execute('''create index var_cadd_scaled_idx on variants(cadd_scaled)''')
    session.execute('''create index var_fitcons_idx on variants(fitcons)''')
    session.execute('''create index var_sv_event_idx on variants(sv_event_id)''')
    # genotype array indices
    session.execute('''create index vid_gt_idx on variants using GIN (gts)''')
    session.execute('''create index vid_gt_types_idx on variants using GIN (gt_types)''')


def index_variation_impacts(session):
    session.execute('''create index varimp_exonic_idx on \
                      variant_impacts(is_exonic)''')
    session.execute('''create index varimp_coding_idx on \
                      variant_impacts(is_coding)''')
    session.execute(
        '''create index varimp_lof_idx on variant_impacts(is_lof)''')
    session.execute('''create index varimp_impact_idx on \
                      variant_impacts(impact)''')
    session.execute('''create index varimp_trans_idx on \
                      variant_impacts(transcript)''')
    session.execute('''create index varimp_gene_idx on \
                      variant_impacts(gene)''')


def index_samples(session):
    session.execute('''create unique index sample_name_idx on samples(name)''')


def index_gene_detailed(session):
    session.execute('''create index gendet_chrom_gene_idx on \
                       gene_detailed(chrom, gene)''')
    session.execute('''create index gendet_rvis_idx on \
                       gene_detailed(rvis_pct)''')
    session.execute('''create index gendet_transcript_idx on \
                       gene_detailed(transcript)''')
    session.execute('''create index gendet_ccds_idx on \
                       gene_detailed(ccds_id)''')

def index_gene_summary(session):
    session.execute('''create index gensum_chrom_gene_idx on \
                       gene_summary(chrom, gene)''')
    session.execute('''create index gensum_rvis_idx on \
                      gene_summary(rvis_pct)''')

def create_indices(session):
    """
    Index our master DB tables for speed
    """
    sys.stderr.write("Trying to index db. Some stuff may explode")
    index_variation(session)
    index_variation_impacts(session)
    index_samples(session)
    index_gene_detailed(session)
    index_gene_summary(session)


def drop_tables(session):
    session.execute("DROP TABLE IF EXISTS variants")
    session.execute("DROP TABLE IF EXISTS variant_impacts")
    session.execute("DROP TABLE IF EXISTS resources")
    session.execute("DROP TABLE IF EXISTS version")
    session.execute("DROP TABLE IF EXISTS gene_detailed")
    session.execute("DROP TABLE IF EXISTS gene_summary")

def create_tables(session):
    """
    Create our master DB tables
    """
    session.execute('''CREATE TABLE if not exists variant_impacts  (   \
                    variant_id int,                               \
                    anno_id int,                                  \
                    gene text,                                        \
                    transcript text,                                  \
                    is_exonic int,                                   \
                    is_coding int,                                   \
                    is_lof int,                                      \
                    exon text,                                        \
                    codon_change text,                                \
                    aa_change text,                                   \
                    aa_length text,                                   \
                    biotype text,                                     \
                    impact text,                                      \
                    impact_so text,                                   \
                    impact_severity text,                             \
                    polyphen_pred text,                               \
                    polyphen_score float,                             \
                    sift_pred text,                                   \
                    sift_score float,                                 \
                    PRIMARY KEY((variant_id, anno_id)))''')

    session.execute('''CREATE TABLE if not exists resources ( \
                     name text PRIMARY KEY,                  \
                     resource text)''')

    session.execute('''CREATE TABLE if not exists version ( \
                     version text PRIMARY KEY)''')
    
    session.execute('''CREATE TABLE if not exists gene_detailed (       \
                   uid int PRIMARY KEY,                                \
                   chrom text,                                         \
                   gene text,                                          \
                   is_hgnc int,                                        \
                   ensembl_gene_id text,                               \
                   transcript text,                                    \
                   biotype text,                                       \
                   transcript_status text,                             \
                   ccds_id text,                                       \
                   hgnc_id text,                                       \
                   entrez_id text,                                     \
                   cds_length text,                                    \
                   protein_length text,                                \
                   transcript_start text,                              \
                   transcript_end text,                                \
                   strand text,                                        \
                   synonym text,                                       \
                   rvis_pct float,                                     \
                   mam_phenotype_id text)''')
    
    session.execute('''CREATE TABLE if not exists gene_summary (     \
                    uid int PRIMARY KEY,                         \
                    chrom text,                                     \
                    gene text,                                      \
                    is_hgnc int,                                   \
                    ensembl_gene_id text,                           \
                    hgnc_id text,                                   \
                    transcript_min_start text,                      \
                    transcript_max_end text,                        \
                    strand text,                                    \
                    synonym text,                                   \
                    rvis_pct float,                                 \
                    mam_phenotype_id text,                          \
                    in_cosmic_census int)''')
    
    session.execute('''CREATE TABLE if not exists vcf_header (vcf_header text PRIMARY KEY)''')


def create_variants_table(session, gt_column_names):

    #TODO: line 230 was hwe decimal(9,7) in sqlite and info was BYTEA
    #Also changed real -> decimal and numeric to float
    placeholders = ",".join(list(repeat("%s",len(gt_column_names))))
    creation =      '''CREATE TABLE if not exists variants  (   \
                    chrom text,                                 \
                    start int,                                  \
                    \"end\" int,                                \
                    vcf_id text,                                \
                    variant_id int PRIMARY KEY,                 \
                    anno_id int,                                \
                    ref text,                                   \
                    alt text,                                   \
                    qual float,                                 \
                    filter text,                                \
                    type text,                                  \
                    sub_type text,                              \
                    call_rate float,                            \
                    in_dbsnp int,                               \
                    rs_ids text ,                   \
                    sv_cipos_start_left int,                    \
                    sv_cipos_end_left int,                      \
                    sv_cipos_start_right int,                   \
                    sv_cipos_end_right int,                     \
                    sv_length int,                              \
                    sv_is_precise boolean,                         \
                    sv_tool text,                               \
                    sv_evidence_type text,                      \
                    sv_event_id text,                           \
                    sv_mate_id text,                            \
                    sv_strand text,                             \
                    in_omim int,                                \
                    clinvar_sig text,              \
                    clinvar_disease_name text,     \
                    clinvar_dbsource text,         \
                    clinvar_dbsource_id text,      \
                    clinvar_origin text,           \
                    clinvar_dsdb text,             \
                    clinvar_dsdbid text,           \
                    clinvar_disease_acc text,      \
                    clinvar_in_locus_spec_db int,               \
                    clinvar_on_diag_assay int,                  \
                    clinvar_causal_allele text,                 \
                    pfam_domain text,                           \
                    cyto_band text,                \
                    rmsk text,                     \
                    in_cpg_island boolean,                         \
                    in_segdup boolean,                             \
                    is_conserved boolean,                          \
                    gerp_bp_score float,                        \
                    gerp_element_pval float,                    \
                    num_hom_ref int,                            \
                    num_het int,                                \
                    num_hom_alt int,                            \
                    num_unknown int,                            \
                    aaf decimal,                                   \
                    hwe decimal,                           \
                    inbreeding_coeff float,                   \
                    pi float,                                 \
                    recomb_rate float,                        \
                    gene text,                                  \
                    transcript text,                            \
                    is_exonic int,                              \
                    is_coding int,                              \
                    is_lof int,                                 \
                    exon text,                                  \
                    codon_change text,                          \
                    aa_change text,                             \
                    aa_length text,                             \
                    biotype text,                               \
                    impact text,                   \
                    impact_so text,                \
                    impact_severity text,                       \
                    polyphen_pred text,                         \
                    polyphen_score float,                       \
                    sift_pred text,                             \
                    sift_score float,                           \
                    anc_allele text,                            \
                    rms_bq float,                               \
                    cigar text,                                 \
                    depth int,                     \
                    strand_bias float,             \
                    rms_map_qual float,            \
                    in_hom_run int,                \
                    num_mapq_zero int,             \
                    num_alleles int,               \
                    num_reads_w_dels float,        \
                    haplotype_score float,         \
                    qual_depth float,              \
                    allele_count int,              \
                    allele_bal float,              \
                    in_hm2 int,                                 \
                    in_hm3 int,                                 \
                    is_somatic int,                             \
                    somatic_score float,                        \
                    in_esp boolean,                                \
                    aaf_esp_ea float,                         \
                    aaf_esp_aa float,                         \
                    aaf_esp_all float,                        \
                    exome_chip boolean,                            \
                    in_1kg boolean,                                \
                    aaf_1kg_amr float,                        \
                    aaf_1kg_eas float,                        \
                    aaf_1kg_sas float,                        \
                    aaf_1kg_afr float,                        \
                    aaf_1kg_eur float,                        \
                    aaf_1kg_all float,                        \
                    grc text,                      \
                    gms_illumina float,                         \
                    gms_solid float,                            \
                    gms_iontorrent float,                       \
                    in_cse boolean,                                \
                    encode_tfbs text,                           \
                    encode_dnaseI_cell_count int,               \
                    encode_dnaseI_cell_list text,               \
                    encode_consensus_gm12878 text,              \
                    encode_consensus_h1hesc text,               \
                    encode_consensus_helas3 text,               \
                    encode_consensus_hepg2 text,                \
                    encode_consensus_huvec text,                \
                    encode_consensus_k562 text,                 \
                    vista_enhancers text,                       \
                    cosmic_ids text,                            \
                    info blob,                                 \
                    cadd_raw float,                             \
                    cadd_scaled float,                          \
                    fitcons float,'''
    insert = (creation + "{0})").format(placeholders) % tuple(gt_column_names)
    session.execute(insert)                

def create_sample_table(session, args):
    NUM_BUILT_IN = 6
    fields = get_ped_fields(args.ped_file)
    required = "sample_id int"
    optional_fields = ["family_id", "name", "paternal_id", "maternal_id",
                       "sex", "phenotype"]
    optional_fields += fields[NUM_BUILT_IN:] + ["PRIMARY KEY(sample_id)"]
    optional = " text,".join(optional_fields)
    structure = '''{0}, {1}'''.format(required, optional)
    creation = "CREATE TABLE if not exists samples ({0})".format(structure)
    session.execute(creation)

def _insert_variation_one_per_transaction(session, variants_buffer):
    for variant in variants_buffer:
        session.execute("BEGIN TRANSACTION")
        session.execute('INSERT INTO variants values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', variant)
        session.execute("END TRANSACTION")
        # skip repeated keys until we get to the failed variant

def insert_variation(session, variant):
    """
    Populate the variants table with each variant in the variant.
    """
    session.execute("BEGIN TRANSACTION")
    session.executemany('INSERT INTO variants values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', variant)

    session.execute("END TRANSACTION")
    
def batch_insert(session, table, columns, contents):
    """
    Populate the given table with the given values
    """
    column_names = ','.join(columns)
    question_marks = ",".join(list(repeat("?",len(columns))))
    insert_query = session.prepare('INSERT INTO ' + table + ' (' + column_names + ') VALUES (' + question_marks + ')')
    batch = BatchStatement()

    for entry in contents:
        batch.add(insert_query, entry)
        
    session.execute(batch)
    
def insert(session, table, columns, contents):
    column_names = ','.join(columns)
    placeholders = ",".join(list(repeat("%s",len(columns))))
    insert_query = 'INSERT INTO ' + table + ' (' + column_names + ') VALUES (' + placeholders + ')'
    session.execute(insert_query, contents)

def insert_sample(session, sample_list):
    """
    Populate the samples with sample ids, names, and
    other indicative information.
    """
    placeholders = ",".join(list(repeat("%s", len(sample_list))))
    session.execute("BEGIN TRANSACTION")
    
    # a hack to prevent loading the same data multiple times in PGSQL mode.
    session.execute("SELECT count(1) FROM samples")
    count = session.fetchone()[0]
    if count == 0:
        session.execute("INSERT INTO samples values "
                       "({0})".format(placeholders), sample_list)
    
    session.execute("END")

def close_and_commit(session, connection):
    """
    Commit changes to the DB and close out DB session.
    """
    print "committing"
    connection.commit()

    #session.execute("""SELECT * FROM variants 
    #                        WHERE gt_types[7] =1 
    #                        AND   gt_types[9] =0 
    #                        AND   gt_types[17] =2""")
    #for row in session:
    #    print row

    #print "closing"
    connection.close()

def empty_tables(session):
    session.execute('''delete * from variation''')
    session.execute('''delete * from samples''')


def update_gene_summary_w_cancer_census(session, genes):
    update_qry = "UPDATE gene_summary SET in_cosmic_census = %s "
    update_qry += " WHERE gene = %s and chrom = %s"
    session.executemany(update_qry, genes)

# @contextlib.contextmanager
# def database_transaction(db):
#     conn = sqlite3.connect(db)
#     conn.isolation_level = None
#     session = conn.session()
#     session.execute('PRAGMA synchronous = OFF')
#     session.execute('PRAGMA journal_mode=MEMORY')
#     yield session
#     conn.commit
#     session.close()
