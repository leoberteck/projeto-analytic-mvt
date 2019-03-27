const fs = require('fs')
    , _ = require('lodash')
    , path = require('path')
    , uuid = require('uuid/v1')
    , rimraf = require("rimraf")
    , log4js = require('log4js')
    , oracle = require('oracledb')
    , sqlite3 = require('sqlite3')
    , tippecanoe = require('tippecanoe')
    , streamifier = require('streamifier')
    , turf_projection = require('@turf/projection')
    , executeQuery = require('./utils.js').executeQuery
    , jsonStreamToObject = require('./utils.js').jsonStreamToObject

const createMap = async (eventId, oracleCon, data) => {

    const workspace = uuid()
        , workspaceDir = path.join(__dirname, workspace)
        , filename = 'map.geojson'
    if (!fs.existsSync(workspaceDir)){
        fs.mkdirSync(workspaceDir);
    }

    const logger = log4js.getLogger(`MAP-CREATOR@${eventId}/${workspace}`)
    const mbtiles = path.join(workspaceDir, `${filename}.mbtiles`)
    
    const clearFiles = (dirname) => {
        return new Promise((resolve) => {
            rimraf(dirname, () => resolve());
        })
    }
    
    const writeDataToFile = (_data, _filename) => {
        return new Promise((resolve, reject) => {
            fs.writeFile(_filename, _data, (err) => {
                if(err){ reject(err) } 
                else { resolve() }
            })
        })
    }

    const insertNewMap = (oracleCon, mapAttributes) => {
        return new Promise((resolve, reject) => {
            oracleCon.execute('INSERT INTO MAPS (MAP_ATTRIBUTES) VALUES (:mapAttributes) RETURNING MAP_ID INTO :id'
                , {
                    mapAttributes: JSON.stringify(mapAttributes)
                    , id: { type: oracle.NUMBER, dir: oracle.BIND_OUT }
                }
                , { autoCommit: true }
                , (err, result) => {
                    if (err) {
                        logger.error(`Could not create a new map id with error:\n`, err)
                        reject(err)
                    } else {
                        logger.info(`Created new map id ${result.outBinds.id}`)
                        resolve(result.outBinds.id[0])
                    }
                }
            )
        })
    }
    
    const each = (sqlitedb, select, callback) => {
        return new Promise((resolve, reject) => {
            sqlitedb.each(select, (err, row) => {
                callback(err, row)
            }
                , (err, count) => {
                    if (err) {
                        logger.error(`Query (${select}) failed with error:\n`, err)
                        reject(err)
                    } else {
                        resolve(count)
                    }
                }
            )
        })
    }
    
    const getSqliteConnection = (filename) => {
        return new Promise((resolve, reject) => {
            var db = new sqlite3.Database(filename, sqlite3.OPEN_READONLY, (err) => {
                if (err) {
                    logger.error(`Unable to connect to sqlite databse ${filename}\n`, err)
                    reject(err)
                }
                else {
                    logger.info(`Connected to Sqlite database ${filename}`)
                    resolve(db)
                }
            })
        })
    }
    
    const insertTileData = (oracleCon, mapId, sqliteRow) => {
        return new Promise((resolve, reject) => {
            oracleCon.execute(
                `INSERT INTO TILES(MAP_ID, ZOOM_LEVEL, TILE_COLUMN, TILE_ROW, TILE_DATA) 
                VALUES (:vlmapId, :vlzoom, :vlcolumn, :vlrow, EMPTY_BLOB()) 
                RETURNING TILE_DATA INTO :lobbv`
                , {
                    vlmapId: mapId
                    , vlzoom: sqliteRow.zoom_level
                    , vlcolumn: sqliteRow.tile_column
                    , vlrow: sqliteRow.tile_row
                    , lobbv: { type: oracle.BLOB, dir: oracle.BIND_OUT }
                }
                , { autoCommit: false }
                , (err, result) => {
                    if (err) {
                        logger.error(`Failed insert into tiles with error:\n`, err)
                        reject(err)
                        return;
                    }
                    if (result.rowsAffected != 1 || result.outBinds.lobbv.length != 1) {
                        logger.error(`Failed insert into tiles with error: No rows affected`)
                        reject()
                        return;
                    }
                    var lobStream = result.outBinds.lobbv[0];
    
                    lobStream.on('close', () => {
                        oracleCon.commit((err) => {
                            if (err) {
                                logger.error(`Could not commit transaction with error:\n`, err)
                                reject(err)
                            } else {
                                logger.info('Tile inserted')
                                resolve()
                            }
                        })
                    })
                    lobStream.on('error', (err) => {
                        logger.error(`Failed to write onto lob stream:\n`, err)
                        reject(err)
                    })
    
                    streamifier.createReadStream(Buffer.from(sqliteRow.tile_data)).pipe(lobStream)
                })
        })
    }

    data = streamifier.createReadStream(Buffer.from(data))

    logger.info('Started converting stream to object')
    var geojsonWgs84 = await jsonStreamToObject(data)
    data = null
    logger.info('Converting to web mercator')
    var geojsonMercator = turf_projection.toMercator(geojsonWgs84)
    geojsonWgs84 = null;

    logger.info('Writing to disk...')
    await writeDataToFile(JSON.stringify(geojsonMercator), path.join(workspaceDir, filename))
    logger.info(`Done writing to ${filename}`)

    logger.info('Generating mvt...')
    tippecanoe([path.join(workspaceDir, filename)], {
        readParallel: true,
        output: mbtiles,
        minimumZoom: 10,
        maximumZoom: 16,
        fullDetail:16,
        noFeatureLimit:true,
        noLineSimplification:true,
        tileStatsAttributesLimit: 999999,
        tileStatsSampleValuesLimit:999999,
        tileStatsValuesLimit:999999,
        force: true,
        projection: 'EPSG:3857' //Web Mercator
    })
    logger.info('Done!')

    var mapAttributes = {}
    const sqlitedb = await getSqliteConnection(mbtiles)
    await each(sqlitedb, 'SELECT name, value FROM metadata', (err, row) => {
        mapAttributes[row.name] = row.value
    })

    var mapId = await insertNewMap(oracleCon, mapAttributes)
    var rows = []

    await each(sqlitedb, 'SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles', (err, row) => {
        rows.push(row)
    })

    while (rows && rows.length) {
        await insertTileData(oracleCon, mapId, rows.pop())
    }

    logger.info(`Processing maps attributes`)
    await executeQuery(oracleCon, `BEGIN PKG_PROCESSAMENTO_ANALITICO.PRC_DESCRIBE_ATTRIBUTES(:map_id); END;`, { map_id: mapId })
        .catch((err) => {
            logger.error(`Could not describe map attributes of map: ${mapId}`, err)
            throw err
        })

    logger.info(`DONE processing maps attributes`)

    logger.info('Clearing Workspace files')
    await clearFiles(workspaceDir)
    logger.info(`DONE clearing files`)
    return mapId;
}

module.exports = createMap;