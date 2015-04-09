check()
{
        if diff $1 $2; then
        echo ok
        else
        echo fail
        fi
}
export -f check

cd test

bash cassandra-test-setup.sh

bash cassandra-test-query.sh

bash test-load.sh

bash test-wildcards.sh

bash test-genotypes.sh

bash test-exac.sh

bash test-genome.sh

bash test-columns.sh
