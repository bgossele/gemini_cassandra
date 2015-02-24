#!/usr/bin/env python

import  cassandra
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
    session.execute('''CREATE TABLE if not exists variants  (    \
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
                    rs_ids text default NULL,                   \
                    sv_cipos_start_left int,                    \
                    sv_cipos_end_left int,                      \
                    sv_cipos_start_right int,                   \
                    sv_cipos_end_right int,                     \
                    sv_length int,                              \
                    sv_is_precise bool,                         \
                    sv_tool text,                               \
                    sv_evidence_type text,                      \
                    sv_event_id text,                           \
                    sv_mate_id text,                            \
                    sv_strand text,                             \
                    in_omim int,                                \
                    clinvar_sig text default NULL,              \
                    clinvar_disease_name text default NULL,     \
                    clinvar_dbsource text default NULL,         \
                    clinvar_dbsource_id text default NULL,      \
                    clinvar_origin text default NULL,           \
                    clinvar_dsdb text default NULL,             \
                    clinvar_dsdbid text default NULL,           \
                    clinvar_disease_acc text default NULL,      \
                    clinvar_in_locus_spec_db int,               \
                    clinvar_on_diag_assay int,                  \
                    clinvar_causal_allele text,                 \
                    pfam_domain text,                           \
                    cyto_band text default NULL,                \
                    rmsk text default NULL,                     \
                    in_cpg_island bool,                         \
                    in_segdup bool,                             \
                    is_conserved bool,                          \
                    gerp_bp_score float,                        \
                    gerp_element_pval float,                    \
                    num_hom_ref int,                            \
                    num_het int,                                \
                    num_hom_alt int,                            \
                    num_unknown int,                            \
                    aaf real,                                   \
                    hwe decimal(9,7),                           \
                    inbreeding_coeff numeric,                   \
                    pi numeric,                                 \
                    recomb_rate numeric,                        \
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
                    impact text default NULL,                   \
                    impact_so text default NULL,                \
                    impact_severity text,                       \
                    polyphen_pred text,                         \
                    polyphen_score float,                       \
                    sift_pred text,                             \
                    sift_score float,                           \
                    anc_allele text,                            \
                    rms_bq float,                               \
                    cigar text,                                 \
                    depth int default NULL,                 \
                    strand_bias float default NULL,             \
                    rms_map_qual float default NULL,            \
                    in_hom_run int default NULL,            \
                    num_mapq_zero int default NULL,         \
                    num_alleles int default NULL,           \
                    num_reads_w_dels float default NULL,        \
                    haplotype_score float default NULL,         \
                    qual_depth float default NULL,              \
                    allele_count int default NULL,          \
                    allele_bal float default NULL,              \
                    in_hm2 int,                                \
                    in_hm3 int,                                \
                    is_somatic int,                            \
                    somatic_score float,                        \
                    in_esp bool,                                \
                    aaf_esp_ea numeric,                    \
                    aaf_esp_aa numeric,                    \
                    aaf_esp_all numeric,                   \
                    exome_chip bool,                            \
                    in_1kg bool,                                \
                    aaf_1kg_amr numeric,                   \
                    aaf_1kg_eas numeric,                   \
                    aaf_1kg_sas numeric,                   \
                    aaf_1kg_afr numeric,                   \
                    aaf_1kg_eur numeric,                   \
                    aaf_1kg_all numeric,                   \
                    grc text default NULL,                      \
                    gms_illumina float,                         \
                    gms_solid float,                            \
                    gms_iontorrent float,                       \
                    in_cse bool,                                \
                    encode_tfbs text,                           \
                    encode_dnaseI_cell_count int,           \
                    encode_dnaseI_cell_list text,               \
                    encode_consensus_gm12878 text,              \
                    encode_consensus_h1hesc text,               \
                    encode_consensus_helas3 text,               \
                    encode_consensus_hepg2 text,                \
                    encode_consensus_huvec text,                \
                    encode_consensus_k562 text,                 \
                    vista_enhancers text,                       \
                    cosmic_ids text,                            \
                    info BYTEA,                                  \
                    cadd_raw float,                             \
                    cadd_scaled float,                          \
                    fitcons float)''')

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
                    uid int PRIMARY KEY                         \
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

def create_sample_table(session, args):
    NUM_BUILT_IN = 6
    fields = get_ped_fields(args.ped_file)
    required = "sample_id int"
    optional_fields = ["family_id", "name", "paternal_id", "maternal_id",
                       "sex", "phenotype"]
    optional_fields += fields[NUM_BUILT_IN:] + ["PRIMARY KEY(sample_id)"]
    optional = " text default NULL,".join(optional_fields)
    structure = '''{0}, {1}'''.format(required, optional)
    creation = "CREATE TABLE if not exists samples ({0})".format(structure)
    session.execute(creation)

def _insert_variation_one_per_transaction(session, buffer):
    for variant in buffer:
        try:
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
        except psycopg2.IntegrityError, e:
            session.execute("END TRANSACTION")
            continue
        except psycopg2.ProgrammingError, e:
            print variant
            print "Error %s:" % (e.args[0])
            sys.exit(1)

def insert_variation(session, buffer):
    """
    Populate the variants table with each variant in the buffer.
    """
    try:
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
                                                         %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', buffer)

        session.execute("END TRANSACTION")
    except psycopg2.ProgrammingError:
        session.execute("END TRANSACTION")
        _insert_variation_one_per_transaction(session, buffer)


def insert_variation_impacts(session, contents):
    """
    Populate the variant_impacts table with each variant in the buffer.
    """
    insert_impact = session.prepare('INSERT INTO variant_impacts VALUES (?,?,?,?,?,?,?,?, \
                                                            ?,?,?,?,?,?,?,?, \
                                                            ?,?,?)')
    batch = BatchStatement()

    for impact in contents:
        batch.add(insert_impact, impact)
        
    session.execute(batch)    


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

#TODO: string with all the columns names
def insert_gene_detailed(session, table_contents):
    insert_gene = session.prepare('INSERT INTO gene_detailed VALUES (?,?,?,?,?,?,?,?,?, \
                                                                     ?,?,?,?,?,?,?,?,?, \
                                                                     ?)')
    batch = BatchStatement()

    for gene in table_contents:
        batch.add(insert_gene, gene)
        
    session.execute(batch)    

def insert_gene_summary(session, contents):
    insert_gene = session.prepare('INSERT INTO gene_summary VALUES (?,?,?,?,?,?,?,?, \
                                                                    ?,?,?,?,?)')
    batch = BatchStatement()

    for gene in contents:
        batch.add(insert_gene, gene)
        
    session.execute(batch)   
    
def insert_resources(session, resources):
    """Populate table of annotation resources used in this database.
    """
    insert_resource = session.prepare('INSERT INTO resources (name, resource) VALUES (?,?)')
    batch = BatchStatement()
    
    for resource in resources:
        batch.add(insert_resource, resource)
        
    session.execute(batch)

def insert_version(session, version):
    """Populate table of documenting which
    gemini version was used for this database.
    """
    session.execute('INSERT INTO version (version) VALUES (%s)', (version,))


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
