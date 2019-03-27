const express = require('express')
    , app = express()
    , oracle = require('oracledb')
    , log4js = require('log4js')
    , cors = require('cors')

log4js.configure({
    appenders: {
        fileOutput: { type: 'file', filename: 'application.log', maxLogSize: 104857600, backups: 5, compress: true }
        , console: { type: 'console' }
    }
    , categories: {
        default: { appenders: ['fileOutput', 'console'], level: 'debug' }
    }
})

const logger = log4js.getLogger('ANALYTIC-BACKEND')

const QUERY_EVENTOS_TALHAO = 
`SELECT
eot.CD_CLIENTE
, eot.CD_ID
, glt.CD_UNIDADE
, glt.CD_FAZENDA
, glt.CD_ZONA
, glt.CD_TALHAO
, eot.CD_OPERACAO
, TO_TIMESTAMP(eot.DT_INICIO) DT_INICIO
, TO_TIMESTAMP(eot.DT_FIM) DT_FIM
FROM EVENTOS_OPERACAO_TALHAO eot
JOIN GEO_LAYER_TALHAO GLT on eot.CD_TALHAO = GLT.CD_ID
WHERE eot.CD_CLIENTE = :cd_cliente
AND glt.CD_UNIDADE = :cd_unidade
AND glt.CD_FAZENDA = :cd_fazenda
AND glt.CD_ZONA    = :cd_zona
AND glt.CD_TALHAO  = :cd_talhao
ORDER BY eot.DT_INICIO DESC, eot.DT_FIM DESC
OFFSET :skip ROWS FETCH NEXT :limit ROWS ONLY`
, QUERY_EVENTO_RECENTES = 
`SELECT
t.CD_CLIENTE
 , t.CD_ID
 , t.CD_UNIDADE
 , t.CD_FAZENDA
 , t.CD_ZONA
 , t.CD_TALHAO
 , t.CD_OPERACAO
 , t.DT_INICIO
 , t.DT_FIM
FROM (
SELECT
 eot.CD_CLIENTE
 , eot.CD_ID
 , glt.CD_UNIDADE
 , glt.CD_FAZENDA
 , glt.CD_ZONA
 , glt.CD_TALHAO
 , eot.CD_OPERACAO
 , TO_TIMESTAMP(eot.DT_INICIO) DT_INICIO
 , TO_TIMESTAMP(eot.DT_FIM) DT_FIM
 , ROW_NUMBER() OVER (PARTITION BY eot.CD_TALHAO ORDER BY DT_INICIO DESC) rn
FROM EVENTOS_OPERACAO_TALHAO eot
 JOIN GEO_LAYER_TALHAO GLT on eot.CD_TALHAO = GLT.CD_ID
WHERE eot.CD_CLIENTE = :cd_cliente
 AND glt.CD_UNIDADE = NVL(:cd_unidade, glt.CD_UNIDADE)
) t
WHERE t.rn = 1`
, QUERY_LAYER_DEFINITION = 
`SELECT
eol.MAP_ID
, eol.JSON_LAYER
FROM EVENTOS_OPERACAO_LAYERS eol
JOIN EVENTOS_OPERACAO_TALHAO EOT on eol.EVENT_ID = EOT.CD_ID
WHERE eot.CD_CLIENTE = :cd_cliente
AND eot.CD_ID = :cd_evento
AND eol.LAYER_TYPE = :layer_type`

const PAGE_SIZE = 50;

(async() => {

    const pool = await oracle.createPool({
        poolMax         : process.env.db_pool_max || 5
        , poolMin       : process.env.db_pool_min || 1
        , user          : process.env.db_username
        , password      : process.env.db_password
        , connectString : process.env.db_connection_string
    })

    app.use(cors())

    app.get('/eventos_talhao/:c/:u/:f/:z/:t', async (req, res) => {
        var conn = await pool.getConnection()
        try{
            var page = (req.query.page || 0)
            var pageSize = (req.query.page_size || PAGE_SIZE)
            var result = await conn.execute(
                QUERY_EVENTOS_TALHAO, {
                    cd_cliente : req.params.c
                    , cd_unidade : req.params.u
                    , cd_fazenda : req.params.f
                    , cd_zona : req.params.z
                    , cd_talhao : req.params.t
                    , skip : page * pageSize
                    , limit : pageSize
                }, {
                    outFormat: oracle.OBJECT
                })
            var result = result.rows && result.rows.length > 0 ? result.rows : []
            res.status(200).send({
                page: page
                , size: result.length
                , content: result
            })
        } catch(err) {
            logger.error(`GET /eventos_talhao/:c/:u/:f/:z/:t - `, err)
            res.status(400).send(err)
        }

        await conn.close()
    })

    app.get('/eventos_talhao/recentes/:c/:u?', async (req, res) => {
        var conn = await pool.getConnection()
        try{
            var result = await conn.execute(
                QUERY_EVENTO_RECENTES, {
                    cd_cliente : req.params.c
                    , cd_unidade : req.params.u
                }, {
                    outFormat: oracle.OBJECT
                })
            res.status(200).send({
                content: result.rows
            })
        } catch(err) {
            logger.error(`GET /eventos_talhao/recentes/:c/:u? - `, err)
            res.status(400).send(err)
        }

        await conn.close()
    })

    app.get('/layers/:c/:event/:layer', async (req, res) => {
        var conn = await pool.getConnection()
        try{
            var result = await conn.execute(
                QUERY_LAYER_DEFINITION, {
                    cd_cliente : req.params.c
                    , cd_evento : req.params.event
                    , layer_type : req.params.layer
                }, {
                    outFormat: oracle.OBJECT
                    , fetchInfo : {
                        JSON_LAYER : { type: oracle.STRING }
                    }
                })
            var layer = result.rows && result.rows.length > 0 ? result.rows[0] : {}
            if(layer.JSON_LAYER){
                layer.JSON_LAYER = JSON.parse(layer.JSON_LAYER)
            }
            res.status(200).send(layer)
        } catch(err) {
            logger.error(`GET /layers/:c/:event/:layer - `, err)
            res.status(400).send(err)
        }

        await conn.close()
    })

    var server = app.listen(4000, () => {
        logger.info(`Server up on port: ${server.address().port}`)
    })
})()