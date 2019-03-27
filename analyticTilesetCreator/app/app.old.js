/*
Gerador de tileset analítico
MVT = Mapbox Vector Tilset
*/
const argv = require('yargs').argv
    , profile = require('./profiles')(argv.profile, argv.operation, argv)

const tippecanoe = require('tippecanoe')
    , oracle = require('oracledb')
    , sqlite3 = require('sqlite3')
    , _ = require('lodash')
    , log4js = require('log4js')
    , fs = require('fs')
    , turf_projection = require('@turf/projection')
    , streamifier = require('streamifier')

log4js.configure({
    appenders: {
        fileOutput: { type: 'file', filename: 'application.log', maxLogSize: 104857600, backups: 5, compress: true }
        , console: { type: 'console' }
    }
    , categories: {
        default: { appenders: ['fileOutput', 'console'], level: 'debug' }
    }
})



var do_work = async (profile) => {

    const logger = log4js.getLogger('ANALYTIC-MVT')

    function jsonStreamToObject(stream) {
        return new Promise((resolve, reject) => {
            var str = ''
            stream.on('data', (chunk) => {
                str += Buffer.from(chunk).toString('utf8')
            })
            stream.on('error', (err) => logger.error('Failed to convert stream to object', err))
            stream.on('end', () => {
                resolve(JSON.parse(str))
                logger.info('Converted stream to object')
            });
        })
    }

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

    function getResultSet(oracleCon, select, parameters = {}) {
        return new Promise((resolve, reject) => {
            oracleCon.execute(select
                , parameters
                , {
                    resultSet: false
                    , outFormat: oracle.OBJECT
                    , fetchInfo: {
                        FEATURE: { type: oracle.BUFFER }
                    }
                }, (err, result) => {
                    err != null ? reject(err) : resolve(result.rows)
                })
        }).then((rs) => {
            logger.info(`ResultSet returned successfully`)
            return rs
        }).catch((err) => {
            logger.error(`Unable to get ResultSet from select ${select}\n`, err)
            process.exit()
        })
    }

    

    logger.info('Starting process ... ')

    const filename = 'map.geojson'
    const mbtiles = `${filename}.mbtiles`

    await clearFiles(filename);

    const oracleCon = await getOracleConnection(profile.db_user, profile.db_pass.toString(), profile.db_url)
    const resultSet = await getResultSet(
        oracleCon
        , profile.query
        , profile.parameters
    )

    logger.info('Reading ResultSet...')
    var data = resultSet[0].GEOJSON

    logger.info('Started converting stream to object')
    var geojsonWgs84 = await jsonStreamToObject(data)
    data = null
    logger.info('Converting to web mercator')
    var geojsonMercator = turf_projection.toMercator(geojsonWgs84)
    geojsonWgs84 = null;

    logger.info('Writing to disk...')
    await writeDataToFile(JSON.stringify(geojsonMercator), filename)

    logger.info(`Done writing to ${filename}`)

    

    return mapId;
}

module.exports = do_work;