const _ = require('lodash')
    , log4js = require('log4js')
    , oracle = require('oracledb')
    , argv = require('yargs').argv
    , createMap = require('./geojsonToMap.js')
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

const logger = log4js.getLogger('GEOJSON-TO-MVT')

function getOracleConnection(dbUser, dbPass, dbUrl) {
    return new Promise((resolve, reject) => {
        oracle.getConnection({
            user: dbUser
            , password: dbPass
            , connectString: dbUrl
        }, (err, connection) => {
            err != null ? reject(err) : resolve(connection)
        })
    }).then((conn) => {
        logger.info(`Connected to oracle Database`)
        return conn
    }).catch((err) => {
        logger.error(`Unable to connect to oracle databse ${dbUser}, with user ${dbPass}\n`, err)
        process.exit()
    })
}

var do_work = async (profile) => {

    logger.info('Starting process ... ')
    const conn = await getOracleConnection(profile.db_user, profile.db_pass.toString(), profile.db_url)

    logger.info('Quering for changed events')
    var result = await executeQuery(
        conn
        , profile.query
        , profile.parameters
        , { 
            resultSet: false
            , outFormat: oracle.OBJECT
            , fetchInfo : {
                GEOJSON : { type: oracle.BUFFER }
            }
        })
        .catch((err) => {
            logger.error('Failed to retrieve changed events.', err)
            process.exit()
        })

    var mapId = await createMap('mvt', conn, result.rows[0].GEOJSON)
        .catch((err) => {
            logger.fatal('Could not process event. ', err)
            conn.close()
        })
    logger.info(`Created map with id ${mapId}`)
    logger.info('=====================Done!!!=====================')
    await oracleCon.commit()
    await conn.close()
}

do_work(require('./profiles.js')(argv.profile, argv.operation, argv))