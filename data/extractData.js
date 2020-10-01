const csv = require('csv-parser')
const fs = require('fs-promise')
var format = require('pg-format')
const { Client } = require('pg')

const connObj = {
  user: 'postgres',
  host: 'localhost',
  database: 'postgres',
  password: '1',
  port: 5432,
}

const client = new Client(connObj)

const readCsv = async (year, file, cb) => {
  let registers = []
  let candidates = []
  candidates.length = 0

  fs.createReadStream(`data\\csv\\${file}`)
    .pipe(csv({ separator: ';' }))
    .on('data', async line => {
      const { NM_CANDIDATO, NR_CPF_CANDIDATO, SG_PARTIDO, SG_UF, NM_UE } = line

      if (NR_CPF_CANDIDATO) {

        const candidato = [ NR_CPF_CANDIDATO, NM_CANDIDATO, SG_PARTIDO, SG_UF, NM_UE ]
        candidates.push(candidato)   

        if (candidates.length == 200000) {
          registers.push(candidates)
          candidates = []
        }
      }
    })   
    .on('end', async () => {
      registers.push(candidates)

      for (register of registers) {
        await saveDb(year, register)
      }

      if (typeof cb === 'function') await cb()
    })
}

const saveDb = async (table, candidates) => {
  try {
    await client.query(format(`INSERT INTO parties."${table}" (cpf, name, party, uf, city) VALUES %L`, candidates))
  } catch (error) {
    console.log(error)
  }  
}

const terminate = _ => {
  console.log('Done!')
  process.exit(1)
}

const saveJson = async (cb) => {
  try {
    const res = await client.query('select a.party as "party2020", b.party as "party2016", count(*) from parties."2020"' +
                                   ' a inner join parties."2016" b on a.cpf = b.cpf and a.party <> b.party' +
                                   ' group by a.party, b.party order by count(*) desc, a.party, b.party')

    const jsonContent = JSON.stringify(res.rows)

    fs.writeFile(`data\\json\\parties.json`, jsonContent, 'utf8', function (err) {
        if (err) {
            console.log(err);
            return console.log(err)
        }
    
        console.log('JSON Saved!')
  
        terminate()
    })
  } catch (error) {
    console.log(error)
  }
}

;(async () => {
  await client.connect()
  await readCsv(2016, 'consulta_cand_2016_BRASIL.csv', 
    async _ => { readCsv(2020, 'consulta_cand_2020_BRASIL.csv', 
      async _ => { await saveJson() }
    )}
  )
})()