import postgres, {Options, Row} from 'postgres'

const uri = "postgresql://postgres.takcrpssopyxprsoxagn:mM98I2LQ6DPOEKZO@aws-0-eu-west-2.pooler.supabase.com:5432/postgres"

const sql = postgres({
    host: "aws-0-eu-west-2.pooler.supabase.com",
    port: 5432,
    database: "postgres",
    user: "postgres.takcrpssopyxprsoxagn",
    password: "mM98I2LQ6DPOEKZO",
    connection: {pool_mode: "session"}
});

type Classification = {
    aok: boolean
    propose?: () => void
}
const classifiers: ((row: Row) => Promise<Classification>)[] = [
    // there is a parent that isn't in the database
    async (row: Row) => {
        // figure out about the parent
        throw false;
    },
    // there is an update (probably two) that changes the parent from A -> B where B does not exist
    // there is a create that has no id that has a parent A
    // THEREFORE - the ID of CREATE(parent A) IS B
// or
    // if you find an (update that changes parent to a parent that does not exist)
    // then if (there is a create that sets parent to the update's before parent shortly before)
    // the identifier of the create is the after parent of the update

   // if there is an update or destroy without an identifier
   // look previous in time to look for CREATE with an AFTER name that matches the update ord estroys BEFORE name
    // and propose the CREATE identifier is the BEFORE identifier

    // there is an id missing
    async (row: Row) => {
        if (!row.identifier) {
            if (row.mode == "CREATE") {
                console.log(`create row without identifier, scanning for ${row.after.name} in a before->name`)
                sql`SELECT identifier, count(*), max('user') as upuser, min('user') as downuser
                    FROM transactions_old
                    WHERE before->'name' = ${row.after.name}
                    AND before->'parent' = ${row.after.parent}
                    AND identifier IS NOT NULL
                    GROUP BY identifier`
                    .then(matches => {
                            if (matches.length > 1) {
                                console.log("we have multiple identifiers: ", matches)
                                sql`SELECT *
                                    FROM transactions_old
                                    WHERE mode = "CREATE"
                                    AND identifier in ('${matches.map(r => r.identifier).join("','")}')
                                    `.then(multimatches => {
                                    console.log("create scan!", multimatches)
                                })
                            }
                            if (matches.length == 0) {
                                return {aok: true}
                            }
                            return matches
                                .map(otherRow => ({
                                    aok: false, propose: () => {
                                        console.log`
                                            INSERT INTO transactions_patch
                                            FROM (id, "user", mode, identifier, created, status, "before", "after", comment)
                                            VALUES
                                                (row.id, row.user, row.mode, ${otherRow.identifier}, row.created
                                                row.status, row.before, row.after, 'named linked before->after suggestion replaced null identifier')
                                        `
                                    }
                                }))
                        }
                    )
            } else {
//                console.log(`sus missing identifier but not create (${row.mode})`)
            }
            return {aok: false};
            // is this the not-generated id case?
            // if the name exists in the clades table it may have been repaired manually
            return {
                aok: false, propose: () => {
                    // generate identifier
                    // find another transaction with the same before name
                    sql`SELECT *
                        FROM transactions_old
                        WHERE `

                    console.log(`${row.id} has no identifier, I found a candidate`)
                    const candidate = {identifier: "five"};
                    console.log(`INSERT INTO old_transactions_patch ()
                                 VALUES () identifier = "${candidate.identifier}"
                                 WHERE id = "${row.id}";`)
                    // query that would find identifier for this
                    // propose a fix entry that would repair it
                }
            }
        }
        throw true
    }
];

sql`SELECT *
    FROM transactions_old`.cursor(200, async a => {
    Promise.allSettled(a.flatMap(r => classifiers.map(c => c(r))))
        .then(psr => psr.filter(p => p.status == "fulfilled"))
        .then(ar => ar.map(psr => psr.value))
        .then(classifications => classifications.forEach(c => c.propose?.()))
}).then(r => console.log("done"));

