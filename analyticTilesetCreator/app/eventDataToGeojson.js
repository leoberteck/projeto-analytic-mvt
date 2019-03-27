const fs = require('fs')
    , path = require('path')
    , uuid = require('uuid/v1')
    , rimraf = require("rimraf")
    , log4js = require('log4js')
    , oracle = require('oracledb')
    , streamifier = require('streamifier')
    , createMap = require('./geojsonToMap.js')
    , executeQuery = require('./utils.js').executeQuery
    , jsonStreamToObject = require('./utils.js').jsonStreamToObject

const deleteOldMaps = async (oracleCon, eventId) => {
    var result = await executeQuery(
        oracleCon
        , `SELECT m.MAP_ID
        FROM EVENTOS_OPERACAO_LAYERS eol
        JOIN MAPS M on eol.MAP_ID = M.MAP_ID
        WHERE eol.EVENT_ID = :event_id
        GROUP BY m.MAP_ID`
        , { event_id: eventId }
        , { resultSet: false, outFormat: oracle.OBJECT }
    ).catch((err) => {
        logger.error('Failed to retrieve maps for deletion.', err)
        throw err
    })

    for(var x=0,count=result.rows.length; x<count; x++){
        var map_id = result.rows[x].MAP_ID
        await executeQuery(oracleCon, 'DELETE FROM MAPS WHERE MAP_ID = :map_id', {map_id: map_id})
        .catch((err) => {
            logger.error(`Failed to delete map: ${map_id}`, err)
            throw err
        })
    }
    return
}

const getLayers = (type, eventId, mapId) => {
    var inserts = []
    switch(type){
        case 'LINHAS':
            inserts = [
                { eventId: eventId, mapId: mapId, layerType: 1 }
                , { eventId: eventId, mapId: mapId, layerType: 2 }
                , { eventId: eventId, mapId: mapId, layerType: 3 }
                , { eventId: eventId, mapId: mapId, layerType: 4 }
                , { eventId: eventId, mapId: mapId, layerType: 5 }
            ]
        break;
        case 'SOBREPOSICAO':
            inserts = [
                { eventId: eventId, mapId: mapId, layerType: 7 }
            ]
        break;
        case 'AREA_TRABALHADA':
            inserts = [
                { eventId: eventId, mapId: mapId, layerType: 8 }
            ]
        break;
        case 'AREA_FALHA':
            inserts = [
                { eventId: eventId, mapId: mapId, layerType: 9 }
            ]
        break;
    }
    return inserts;
}

const processEvent = async (oracleCon, event) => {
    
    const logger = log4js.getLogger(`EVENT-PROCESSOR@${event.CD_ID}`)

    await executeQuery(oracleCon, `UPDATE EVENTOS_OPERACAO_TALHAO SET STATUS = :status WHERE CD_ID = :cd_id`, {cd_id : event.CD_ID, status: 'IN_PROCESS'})
        .catch((err) => {
            logger.error(`Failed to update status of event to IN_PROCESS`, err)
            throw err
        })
    
    var erromessage
    try {
        logger.info(`Deleting old maps...`)
        await deleteOldMaps(oracleCon, event.CD_ID)
        logger.info(`DONE Deleting old maps...`)

        erromessage = `Failed to generate geojsons`
        var rGeojson = await executeQuery(
            oracleCon
            , `SELECT * FROM TABLE(PKG_PROCESSAMENTO_ANALITICO.FNC_GEOJSON_EVENTO(:cdCliente, :cdEvento))`
            , {
                cdCliente: { dir: oracle.BIND_IN, type: oracle.NUMBER, val: event.CD_CLIENTE  }
                , cdEvento: { dir: oracle.BIND_IN, type: oracle.NUMBER, val: event.CD_ID  }
            }
            , {
                resultSet: false
                , outFormat: oracle.OBJECT
                , fetchInfo: {
                    TYPE: { type: oracle.STRING }
                    , GEOJSON: { type: oracle.BUFFER }
                }
            })
        logger.info(`Geojson successfully created`)

        for(var x=0, count = rGeojson.rows.length; x < count; x++){

            var geojson = rGeojson.rows[x]

            logger.info(`Processing geojson of type ${geojson.TYPE}`)
            // criar o mapa passando geojson.GEOJSON
            
            erromessage = 'Could not create and insert map'
            var mapId = await createMap(event.CD_ID, oracleCon, geojson.GEOJSON)

            logger.info(`Inserting layer types for map`)
            var inserts = getLayers(geojson.TYPE, event.CD_ID, mapId)
            erromessage = 'Could not insert layers'
            await executeQuery(oracleCon, `INSERT INTO EVENTOS_OPERACAO_LAYERS(EVENT_ID, MAP_ID, LAYER_TYPE) VALUES(:eventId, :mapId, :layerType)`, inserts)    
            logger.info(`DONE Inserting layer types for map`)

            logger.info(`Creating layers`)
            erromessage = 'Could not build map layers'
            await executeQuery(oracleCon, `BEGIN PKG_PROCESSAMENTO_ANALITICO.PRC_BUILD_LAYERS(:map_id, :event_id); END;`, {map_id: mapId, event_id : event.CD_ID})
            logger.info(`DONE Creating layers`)
            
            logger.info('=====================Done!!!=====================')
        }

        erromessage = `Failed to update status of event to UPTODATE`
        await executeQuery(oracleCon, `UPDATE EVENTOS_OPERACAO_TALHAO SET STATUS = :status WHERE CD_ID = :cd_id`, {cd_id : event.CD_ID, status: 'UPTODATE'})

        await oracleCon.commit()
        await oracleCon.close()
    } catch(err){
        logger.error(erromessage, err)
        await executeQuery(oracleCon, `UPDATE EVENTOS_OPERACAO_TALHAO SET STATUS = :status WHERE CD_ID = :cd_id`, {cd_id : event.CD_ID, status: 'CHANGED'}).catch((err) => {
            logger.error(`Failed to update status of event to CHANGED`, err)
        })
        await oracleCon.commit()
        await oracleCon.close()
        throw err
    }
    return 1;
}

module.exports = processEvent