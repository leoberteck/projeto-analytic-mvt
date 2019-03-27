/*
Gerador de tileset analítico
MVT = Mapbox Vector Tilset
*/

const _ = require('lodash')
    , log4js = require('log4js')
    , oracle = require('oracledb')
    , argv = require('yargs').argv
    , processEvent = require('./eventDataToGeojson.js')
    , executeQuery = require('./utils.js').executeQuery

log4js.configure({
    appenders: {
        fileOutput: { type: 'file', filename: 'application.log', maxLogSize: 104857600, backups: 5, compress: true }
        , console: { type: 'console' }
    }
    , categories: {
        default: { appenders: ['fileOutput', 'console'], level: 'debug' }
    }
})

const logger = log4js.getLogger('ANALYTIC-MVT')

function createConnectionPool(username, password, url){
    return new Promise((resolve, reject) => {
        oracle.createPool({
            poolMax: 5
            , user : username
            , password : password
            , connectString : url
        }, (err, pool) => {
            if(err){
                logger.error('Could not create connection pool ', err)
                reject(err)
            } else {
                pool.terminate
                logger.info('Successfully created connection pool')
                resolve(pool)
            }
        })
    })
}

var do_work = async (profile) => {

    logger.info('Starting process ... ')

    const filename = 'map.geojson'
    const pool = await createConnectionPool(profile.db_user, profile.db_pass.toString(), profile.db_url)

    logger.info('Quering for changed events')
    var conn = await pool.getConnection()
    var result = await executeQuery(
        conn
        , `SELECT * FROM EVENTOS_OPERACAO_TALHAO WHERE STATUS = 'CHANGED'`
        , {}
        , { resultSet: false, outFormat: oracle.OBJECT})
        .catch((err) => {
            logger.error('Failed to retrieve changed events.', err)
            process.exit()
        })
        
    conn.close()
    logger.info(`${result.rows ? result.rows.length : 0} events need to be reprocessed.`)
    
    var groups = _.chunk(result.rows, 3)

    for(const group of result.rows){
        // var promises = [] 
        // for(var y=0,ycount=group.length; y<ycount;y++){
        //     conn = await pool.getConnection()
        //     promises.push(processEvent(conn, group[y]))
        // }
        // conn = null
        // var r = await Promise.all(promises)
        conn = await pool.getConnection()
        var r = await processEvent(conn, group)
            .catch((err) => {
                logger.fatal('Could not process event. ', group, err)
            })
    }
    await pool.close(10)
}

async function recursiveWord(){
    await do_work(require('./profiles.js')(argv.profile, null, argv))
    setTimeout(recursiveWord, 60000)
}

recursiveWord();