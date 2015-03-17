import abc
from gemini.gemini_subjects import get_subjects
from gemini.GeminiQuery import GeminiQuery
import re
from gemini.sql_utils import ensure_columns
from gemini_constants import HET, HOM_ALT, HOM_REF, UNKNOWN

import json
import os
import sys
import collections


# gemini imports
class RowFormat:
    """A row formatter to output rows in a custom format.  To provide
    a new output format 'foo', implement the class methods and set the
    name field to foo.  This will automatically add support for 'foo' to
    anything accepting the --format option via --format foo.
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def name(self):
        return

    @abc.abstractmethod
    def format(self, row):
        """ return a string representation of a GeminiRow object
        """
        return '\t'.join([str(row.row[c]) for c in row.row])

    @abc.abstractmethod
    def format_query(self, query):
        """ augment the query with columns necessary for the format or else just
        return the untouched query
        """
        return query

    @abc.abstractmethod
    def predicate(self, row):
        """ the row must pass this additional predicate to be output. Just
        return True if there is no additional predicate"""
        return True

    @abc.abstractmethod
    def header(self, fields):
        """ return a header for the row """
        return "\t".join(fields)

class DefaultRowFormat(RowFormat):

    name = "default"

    def __init__(self, args):
        pass

    def format(self, row):
        return '\t'.join([str(row.row[c]) for c in row.row])

    def format_query(self, query):
        return query

    def predicate(self, row):
        return True

    def header(self, fields):
        """ return a header for the row """
        return "\t".join(fields)

class CarrierSummary(RowFormat):
    """
    Generates a count of the carrier/noncarrier status of each feature in a given
    column of the sample table

    Assumes None == unknown.
    """
    name = "carrier_summary"

    def __init__(self, args):
        subjects = get_subjects(args)
        self.carrier_summary = args.carrier_summary

        # get the list of all possible values in the column
        # but don't include None, since we are treating that as unknown.
        self.column_types = list(set([getattr(x, self.carrier_summary)
                                      for x in subjects.values()]))
        self.column_types = [i for i in self.column_types if i is not None]
        self.column_counters = {None: set()}
        for ct in self.column_types:
            self.column_counters[ct] = set([k for (k, v) in subjects.items() if
                                            getattr(v, self.carrier_summary) == ct])


    def format(self, row):
        have_variant = set(row.variant_samples)
        have_reference = set(row.HOM_REF_samples)
        unknown = len(set(row.UNKNOWN_samples).union(self.column_counters[None]))
        carrier_counts = []
        for ct in self.column_types:
            counts = len(self.column_counters[ct].intersection(have_variant))
            carrier_counts.append(counts)
        for ct in self.column_types:
            counts = len(self.column_counters[ct].intersection(have_reference))
            carrier_counts.append(counts)

        carrier_counts.append(unknown)
        carrier_counts = map(str, carrier_counts)
        return '\t'.join([str(row.row[c]) for c in row.row] + carrier_counts)

    def format_query(self, query):
        return query

    def predicate(self, row):
        return True

    def header(self, fields):
        """ return a header for the row """
        header_columns = self.column_types
        if self.carrier_summary == "affected":
            header_columns = self._rename_affected()
        carriers = [x + "_carrier" for x in map(str, header_columns)]
        noncarriers = [ x + "_noncarrier" for x in map(str, header_columns)]
        fields += carriers
        fields += noncarriers
        fields += ["unknown"]
        return "\t".join(fields)

    def _rename_affected(self):
        header_columns = []
        for ct in self.column_types:
            if ct == True:
                header_columns.append("affected")
            elif ct == False:
                header_columns.append("unaffected")
        return header_columns



class TPEDRowFormat(RowFormat):

    X_PAR_REGIONS = [(60001, 2699520), (154931044, 155260560)]
    Y_PAR_REGIONS = [(10001, 2649520), (59034050, 59363566)]

    name = "tped"
    NULL_GENOTYPES = ["."]
    PED_MISSING = ["0", "0"]
    VALID_CHROMOSOMES = map(str, range(1, 23)) + ["X", "Y", "XY", "MT"]
    POSSIBLE_HAPLOID = ["X", "Y"]

    def __init__(self, args):
        gq = GeminiQuery(args.db)
        subjects = get_subjects(args, skip_filter=True)
        # get samples in order of genotypes
        self.samples = [gq.idx_to_sample_object[x] for x in range(len(subjects))]

    def format(self, row):
        VALID_CHROMOSOMES = map(str, range(1, 23)) + ["X", "Y", "XY", "MT"]
        chrom = row['chrom'].split("chr")[1]
        chrom = chrom if chrom in VALID_CHROMOSOMES else "0"
        start = str(row.row['start'])
        """end = str(row.row['end'])
        ref = row['ref']
        alt = row['alt']"""
        geno = [re.split('\||/', x) for x in row.row['gts'].split(",")]
        geno = [self._fix_genotype(chrom, start, genotype, self.samples[i].sex)
                for i, genotype in enumerate(geno)]
        genotypes = " ".join(list(flatten(geno)))
        name = str(row['variant_id'])
        return " ".join([chrom, name, "0", start, genotypes])

    def format_query(self, query):
        NEED_COLUMNS = ["chrom", "rs_ids", "start", "ref", "alt", "gts", "type", "variant_id"]
        return ensure_columns(query, NEED_COLUMNS)

    def predicate(self, row):
        geno = [re.split("\||/", x) for x in row['gts']]
        geno = list(flatten(geno))
        num_alleles = len(set(geno).difference(self.NULL_GENOTYPES))
        return num_alleles > 0 and num_alleles <= 2 and row['type'] != "sv"

    def _is_haploid(self, genotype):
        return len(genotype) < 2

    def _has_missing(self, genotype):
        return any([allele in self.NULL_GENOTYPES for allele in genotype])

    def _is_heterozygote(self, genotype):
        return len(genotype) == 2 and (genotype[0] != genotype[1])

    def _in_PAR(self, chrom, start):
        if chrom == "X":
            for region in self.X_PAR_REGIONS:
                if start > region[0] and start < region[1]:
                    return True
        elif chrom == "Y":
            for region in self.Y_PAR_REGIONS:
                if start > region[0] and start < region[1]:
                    return True
        return False

    def _fix_genotype(self, chrom, start, genotype, sex):
        """
        the TPED format has to have both alleles set, even if it is haploid.
        this fixes that setting Y calls on the female to missing,
        heterozygotic calls on the male non PAR regions to missing and haploid
        calls on non-PAR regions to be the haploid call for both alleles
        """
        if sex == "2":
            # set female Y calls and haploid calls to missing
            if self._is_haploid(genotype) or chrom == "Y" or self._has_missing(genotype):
                return self.PED_MISSING
            return genotype
        if chrom in self.POSSIBLE_HAPLOID and sex == "1":
            # remove the missing genotype calls
            genotype = [x for x in genotype if x not in self.NULL_GENOTYPES]
            # if all genotypes are missing skip
            if not genotype:
                return self.PED_MISSING
            # heterozygote males in non PAR regions are a mistake
            if self._is_heterozygote(genotype) and not self._in_PAR(chrom, start):
                return self.PED_MISSING
            # set haploid males to be homozygous for the allele
            if self._is_haploid(genotype):
                return [genotype[0], genotype[0]]

        # if a genotype is missing or is haploid set it to missing
        if self._has_missing(genotype) or self._is_haploid(genotype):
            return self.PED_MISSING
        else:
            return genotype

    def header(self, fields):
        return None

class JSONRowFormat(RowFormat):

    name = "json"

    def __init__(self, args):
        pass

    def format(self, row):
        """Emit a JSON representation of a given row
        """
        return json.dumps(row.row)

    def format_query(self, query):
        return query

    def predicate(self, row):
        return True

    def header(self, fields):
        return None

class VCFRowFormat(RowFormat):

    name = "vcf"

    def __init__(self, args):
        self.gq = GeminiQuery(args.db)

    def format(self, row):
        """Emit a VCF representation of a given row

           TODO: handle multiple alleles
        """
        # core VCF fields
        vcf_rec = [row.row['chrom'], row.row['start'] + 1]
        if row.row['vcf_id'] is None:
            vcf_rec.append('.')
        else:
            vcf_rec.append(row.row['vcf_id'])
        vcf_rec += [row.row['ref'], row.row['alt'], row.row['qual']]
        if row.row['filter'] is None:
            vcf_rec.append('PASS')
        else:
            vcf_rec.append(row.row['filter'])
        vcf_rec += [row.row['info'], 'GT']

        # construct genotypes
        gts = list(row['gts'])
        gt_types = list(row['gt_types'])
        gt_phases = list(row['gt_phases'])
        for idx, gt_type in enumerate(gt_types):
            phase_char = '/' if not gt_phases[idx] else '|'
            gt = gts[idx]
            alleles = gt.split(phase_char)
            if gt_type == HOM_REF:
                vcf_rec.append('0' + phase_char + '0')
            elif gt_type == HET:
                # if the genotype is phased, need to check for 1|0 vs. 0|1
                if gt_phases[idx] and alleles[0] != row.row['ref']:
                    vcf_rec.append('1' + phase_char + '0')
                else:
                    vcf_rec.append('0' + phase_char + '1')
            elif gt_type == HOM_ALT:
                vcf_rec.append('1' + phase_char + '1')
            elif gt_type == UNKNOWN:
                vcf_rec.append('.' + phase_char + '.')

        return '\t'.join([str(c) if c is not None else "." for c in vcf_rec])

    def format_query(self, query):
        return query

    def predicate(self, row):
        return True

    def header(self, fields):
        """Return the original VCF's header
        """
        try:
            self.gq.run('select vcf_header from vcf_header')
            return str(self.gq.next()).strip()
        except:
            sys.exit("Your database does not contain the vcf_header table. Therefore, you cannot use --header.\n")
            
            
            
def flatten(l):
    """
    flatten an irregular list of lists
    example: flatten([[[1, 2, 3], [4, 5]], 6]) -> [1, 2, 3, 4, 5, 6]
    lifted from: http://stackoverflow.com/questions/2158395/

    """
    for el in l:
        if isinstance(el, collections.Iterable) and not isinstance(el,
                                                                   basestring):
            for sub in flatten(el):
                yield sub
        else:
            yield el
