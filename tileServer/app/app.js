const express = require('express')
    , oracle = require('oracledb')
    , log4js = require('log4js')
    , argv = require('yargs').argv
    , cors = require('cors')
    , app = express()

log4js.configure({
    appenders : { 
        fileOutput : { type: 'file', filename: 'application.log', maxLogSize:104857600, backups: 5, compress: true }
        , console: { type: 'console' }
    }
    , categories : {
        default : { appenders : [ 'fileOutput', 'console' ], level : 'debug' }
    }
})

const logger = log4js.getLogger('ANALYTIC-TILE-SERVER')

function getOracleConnection(dbUser, dbPass, dbUrl) {
    return new Promise((resolve, reject) => {
        oracle.getConnection({
            user : dbUser
            , password : dbPass
            , connectString : dbUrl
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

function getResultSet(oracleCon, select, parameters = {}){
    return new Promise((resolve, reject) => {
        oracleCon.execute(select
        , parameters
        , {
            resultSet : false
            , outFormat : oracle.OBJECT
            , fetchInfo: {
                TILE_DATA: { type: oracle.BUFFER }
            }
        }, (err, result) => {
            err != null ? reject(err) : resolve(result.rows)
        })  
    }).then((rs) => {
        return rs 
    }).catch((err) => {
        logger.error(`Unable to get ResultSet from select ${select}\n`, err)
        process.exit()
    })
}

async function getBinData(oracleCon, mapId, z, x, y){
    const query = 
    `select TILE_DATA from TILES 
    where MAP_ID = :mapId
    and ZOOM_LEVEL = :z
    and TILE_COLUMN = :x 
    and TILE_ROW = :y`
    var parameters = {
        mapId   : { dir: oracle.BIND_IN, val: mapId , type: oracle.NUMBER }
        , x     : { dir: oracle.BIND_IN, val: x , type: oracle.NUMBER }
        , y     : { dir: oracle.BIND_IN, val: y , type: oracle.NUMBER }
        , z     : { dir: oracle.BIND_IN, val: z , type: oracle.NUMBER }
    }
    var rs = await getResultSet(oracleCon, query, parameters)
    return rs && Array.isArray(rs) && rs.length > 0 && rs[0] ? rs[0].TILE_DATA : null
}

(async () => {

    //connect to oracle map database
    const oracleCon = await getOracleConnection(argv.db_user, argv.db_pass.toString(), argv.db_url)
    //enable cross origin resource sharing
    app.use(cors())
    //route for tile data
    app.get('/:mapId/:z/:x/:y.vector.pbf', async (req, res) => {
            
        //pase url parameters to int
        var mapId = parseInt(req.params.mapId)
        var z = parseInt(req.params.z)
        var x = parseInt(req.params.x)
        var y = parseInt(req.params.y)
        //invert y axis in order to convert map scheme from tms to xyz
        y = Math.pow(2, z) - y - 1;

        var data = await getBinData(oracleCon, mapId, z, x, y)
        if(data == null) {
            logger.warn(`Tile not found map:${mapId} z:${z} x:${x} y:${y}`)
            res.status(404).send(null)
        } else {
            logger.info(`Tile found map:${mapId} z:${z} x:${x} y:${y}`)
            var buff = Buffer.from(data)
            res.writeHead(200, {
                'Content-Type': 'application/x-protobuf',
                'Content-Length': buff.length,
                'Content-Encoding': 'gzip'
            })
            res.write(buff)
            res.end()
        }
    })

    var server = app.listen(argv.server_port, (err) => {
        if(err){
            logger.error(`Server failed to start on port: ${argv.server_port}`, err)
        } else {
            logger.info(`Server up on port: ${server.address().port}`)
        }
    })
})()