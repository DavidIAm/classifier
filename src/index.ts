import postgres, {Options, Row} from 'postgres'

const uri = "postgresql://postgres.takcrpssopyxprsoxagn:mM98I2LQ6DPOEKZO@aws-0-eu-west-2.pooler.supabase.com:5432/postgres"

const sql = postgres({
    host: "aws-0-eu-west-2.pooler.supabase.com",
    port: 5432,
    database: "postgres",
    user: "postgres.takcrpssopyxprsoxagn",
    password: "mM98I2LQ6DPOEKZO",
    connection: { pool_mode: "session" }
});


const classifiers: ((row: Row) => Promise<void>)[] = [];
sql`SELECT * FROM transactions_old`.cursor(2, async a =>
    await Promise.all(a.flatMap(r => classifiers.map(c => c(r))))
).then(() => console.log("done"));
