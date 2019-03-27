const oracle = require('oracledb')

var profiles = {
    development : (props) => ({
        db_user: props.db_user || "SGPA_MAP"
        , db_pass: props.db_pass || "1337"
        , db_url: props.db_url || "localhost:1522/EE"
    })
    , mapa_analitico_linhas_1talhao_1operacao : (props) => ({
        query : `SELECT JSON_OBJECT('type' VALUE 'FeatureCollection'
        , 'features' VALUE JSON_ARRAYAGG(
          JSON_OBJECT('type' VALUE 'Feature'
            , 'properties' VALUE JSON_OBJECT(
              'cdUnidade' VALUE CD_UNIDADE
              , 'cdFazenda' VALUE CD_FAZENDA
              , 'cdZona' VALUE CD_ZONA
              , 'cdTalhao' VALUE CD_TALHAO
              , 'dtHrUtcInicial' VALUE TO_EPOCH(DT_HR_UTC_INICIAL)
              , 'dtHrUtcFinal' VALUE TO_EPOCH(DT_HR_UTC_FINAL)
              , 'cdOperacao' VALUE CD_OPERACAO
              , 'cdOperacParada' VALUE CD_OPERAC_PARADA
              , 'fgTpEquipamento' VALUE FG_TP_EQUIPAMENTO
              , 'cdEquipamento' VALUE CD_EQUIPAMENTO
              , 'cdImplemento' VALUE CD_IMPLEMENTO
              , 'cdEquipe' VALUE CD_EQUIPE
              , 'cdOperador' VALUE CD_OPERADOR
              , 'cdEstado' VALUE CD_ESTADO
              , 'descOperacao' VALUE DESC_OPERACAO
              , 'descEquipamento' VALUE DESC_EQUIPAMENTO
              , 'descOperador' VALUE DESC_OPERADOR
              , 'descTipoBico' VALUE DESC_TIPO_BICO
              , 'descOperacParada' VALUE DESC_OPERAC_PARADA
              , 'fgFrenteTrabalho' VALUE FG_FRENTE_TRABALHO
              , 'cdTipoBico' VALUE CD_TIPO_BICO
              , 'vlTempoSegundos' VALUE VL_TEMPO_SEGUNDOS
              , 'vlArea' VALUE VL_AREA
              , 'vlDistanciaMetros' VALUE VL_DISTANCIA_METROS
              , 'vlVelocidade' VALUE VL_VELOCIDADE
              , 'vlVelocidadeVento' VALUE VL_VELOCIDADE_VENTO
              , 'vlTemperatura' VALUE VL_TEMPERATURA
              , 'vlUmidade' VALUE VL_UMIDADE
              , 'vlConsumoInstantaneo' VALUE VL_CONSUMO_INSTANTANEO
              , 'vlRpm' VALUE VL_RPM
              , 'vlTemperaturaMotor' VALUE VL_TEMPERATURA_MOTOR
              , 'vlOrdemServico' VALUE VL_ORDEM_SERVICO
              , 'vlAlarme' VALUE VL_ALARME
              , 'vlAlertaClima' VALUE VL_ALERTA_CLIMA
              , 'vlAlarmeClima' VALUE VL_ALARME_CLIMA
              , 'vlHectaresHora' VALUE VL_HECTARES_HORA
              , 'vlHorimetroInicial' VALUE VL_HORIMETRO_INICIAL
              , 'vlHorimetroFinal' VALUE VL_HORIMETRO_FINAL
              , 'vlLarguraImplemento' VALUE VL_LARGURA_IMPLEMENTO
              , 'vlRendimentoColheita' VALUE VL_RENDIMENTO_COLHEITA
              , 'vlUmidadeGraos' VALUE VL_UMIDADE_GRAOS
              , 'vlPontoOrvalho' VALUE VL_PONTO_ORVALHO
              , 'vlVazaoLitrosHa' VALUE VL_VAZAO_LITROS_HA
              , 'vlVazaoLitrosMin' VALUE VL_VAZAO_LITROS_MIN
              , 'vlDosagemTaxa1' VALUE VL_DOSAGEM_TAXA1
              , 'vlDosagemTaxa2' VALUE VL_DOSAGEM_TAXA2
              , 'vlDosagemTaxa3' VALUE VL_DOSAGEM_TAXA3
              , 'vlPressaoBomba' VALUE VL_PRESSAO_BOMBA
              ABSENT ON NULL )
              , 'geometry' VALUE MDSYS.SDO_UTIL.TO_GEOJSON(LINHA) FORMAT JSON RETURNING BLOB
            ) RETURNING BLOB
        ) RETURNING BLOB
        ) GEOJSON
        FROM (
          WITH insideTalhao as (
            SELECT *
            FROM GEO_LAYER_OPERACAO_TALHAO
            WHERE CD_UNIDADE = :cdUnidade
              AND CD_FAZENDA = :cdFazenda
              AND CD_ZONA = :cdZona
              AND CD_TALHAO = :cdTalhao
              AND CD_OPERACAO_CB = :cdOperacao
              AND (TO_DATE(:dtStart, 'YYYY-MM-DD HH24:MI:SS') BETWEEN DT_HR_UTC_INICIAL AND DT_HR_UTC_FINAL OR
                   TO_DATE(:dtEnd, 'YYYY-MM-DD HH24:MI:SS') BETWEEN DT_HR_UTC_INICIAL AND DT_HR_UTC_FINAL OR
                   (DT_HR_UTC_INICIAL BETWEEN TO_DATE(:dtStart, 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE(:dtEnd, 'YYYY-MM-DD HH24:MI:SS') AND
                    DT_HR_UTC_FINAL BETWEEN TO_DATE(:dtStart, 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE(:dtEnd, 'YYYY-MM-DD HH24:MI:SS'))
                )
          ) select * from insideTalhao
          union all
          select glot.* from
            GEO_LAYER_OPERACAO_TALHAO glot
            , insideTalhao it
          where it.CD_MAPA_VAR = glot.CD_MAPA_VAR
          AND it.CD_UNIDADE = 0
          AND it.CD_FAZENDA = 0
          AND it.CD_ZONA = 0
          AND it.CD_TALHAO = 0
        ) t WHERE t.LINHA IS NOT NULL`
        , parameters : {
            cdCliente:      { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_cliente  }
            , cdUnidade:    { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_unidade  }
            , cdFazenda:    { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_fazenda  }
            , cdZona:       { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_zona     }
            , cdTalhao:     { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_talhao   }
            , cdOperacao:   { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_operacao }
            , dtStart:      { dir: oracle.BIND_IN, type: oracle.STRING, val: props.dt_start    }
            , dtEnd:        { dir: oracle.BIND_IN, type: oracle.STRING, val: props.dt_end      }
        }
    })
    , mapa_analitico_sobreposicao_1talhao_1operacao : (props) => ({
        query : `SELECT JSON_OBJECT('type' VALUE 'FeatureCollection'
        , 'features' VALUE JSON_ARRAYAGG(
          JSON_OBJECT('type' VALUE 'Feature'
            , 'properties' VALUE JSON_OBJECT(
              'cdUnidade' VALUE CD_UNIDADE
              , 'cdFazenda' VALUE CD_FAZENDA
              , 'cdZona' VALUE CD_ZONA
              , 'cdTalhao' VALUE CD_TALHAO
              , 'dtHrUtcInicial' VALUE TO_EPOCH(DT_HR_UTC_INICIAL)
              , 'dtHrUtcFinal' VALUE TO_EPOCH(DT_HR_UTC_FINAL)
              , 'cdOperacao' VALUE CD_OPERACAO
              , 'cdOperacParada' VALUE CD_OPERAC_PARADA
              , 'fgTpEquipamento' VALUE FG_TP_EQUIPAMENTO
              , 'cdEquipamento' VALUE CD_EQUIPAMENTO
              , 'cdImplemento' VALUE CD_IMPLEMENTO
              , 'cdEquipe' VALUE CD_EQUIPE
              , 'cdOperador' VALUE CD_OPERADOR
              , 'cdEstado' VALUE CD_ESTADO
              , 'descOperacao' VALUE DESC_OPERACAO
              , 'descEquipamento' VALUE DESC_EQUIPAMENTO
              , 'descOperador' VALUE DESC_OPERADOR
              , 'descTipoBico' VALUE DESC_TIPO_BICO
              , 'descOperacParada' VALUE DESC_OPERAC_PARADA
              , 'fgFrenteTrabalho' VALUE FG_FRENTE_TRABALHO
              , 'cdTipoBico' VALUE CD_TIPO_BICO
              , 'vlTempoSegundos' VALUE VL_TEMPO_SEGUNDOS
              , 'vlArea' VALUE VL_AREA
              , 'vlDistanciaMetros' VALUE VL_DISTANCIA_METROS
              , 'vlVelocidade' VALUE VL_VELOCIDADE
              , 'vlVelocidadeVento' VALUE VL_VELOCIDADE_VENTO
              , 'vlTemperatura' VALUE VL_TEMPERATURA
              , 'vlUmidade' VALUE VL_UMIDADE
              , 'vlConsumoInstantaneo' VALUE VL_CONSUMO_INSTANTANEO
              , 'vlRpm' VALUE VL_RPM
              , 'vlTemperaturaMotor' VALUE VL_TEMPERATURA_MOTOR
              , 'vlOrdemServico' VALUE VL_ORDEM_SERVICO
              , 'vlAlarme' VALUE VL_ALARME
              , 'vlAlertaClima' VALUE VL_ALERTA_CLIMA
              , 'vlAlarmeClima' VALUE VL_ALARME_CLIMA
              , 'vlHectaresHora' VALUE VL_HECTARES_HORA
              , 'vlHorimetroInicial' VALUE VL_HORIMETRO_INICIAL
              , 'vlHorimetroFinal' VALUE VL_HORIMETRO_FINAL
              , 'vlLarguraImplemento' VALUE VL_LARGURA_IMPLEMENTO
              , 'vlRendimentoColheita' VALUE VL_RENDIMENTO_COLHEITA
              , 'vlUmidadeGraos' VALUE VL_UMIDADE_GRAOS
              , 'vlPontoOrvalho' VALUE VL_PONTO_ORVALHO
              , 'vlVazaoLitrosHa' VALUE VL_VAZAO_LITROS_HA
              , 'vlVazaoLitrosMin' VALUE VL_VAZAO_LITROS_MIN
              , 'vlDosagemTaxa1' VALUE VL_DOSAGEM_TAXA1
              , 'vlDosagemTaxa2' VALUE VL_DOSAGEM_TAXA2
              , 'vlDosagemTaxa3' VALUE VL_DOSAGEM_TAXA3
              , 'vlPressaoBomba' VALUE VL_PRESSAO_BOMBA
              ABSENT ON NULL )
              , 'geometry' VALUE MDSYS.SDO_UTIL.TO_GEOJSON(GEOMETRIA) FORMAT JSON RETURNING BLOB
            ) RETURNING BLOB
        ) RETURNING BLOB
        ) GEOJSON
        FROM (
          WITH insideTalhao as (
            SELECT *
            FROM GEO_LAYER_OPERACAO_TALHAO
            WHERE CD_UNIDADE = :cdUnidade
              AND CD_FAZENDA = :cdFazenda
              AND CD_ZONA = :cdZona
              AND CD_TALHAO = :cdTalhao
              AND CD_OPERACAO_CB = :cdOperacao
              AND (TO_DATE(:dtStart, 'YYYY-MM-DD HH24:MI:SS') BETWEEN DT_HR_UTC_INICIAL AND DT_HR_UTC_FINAL OR
                   TO_DATE(:dtEnd, 'YYYY-MM-DD HH24:MI:SS') BETWEEN DT_HR_UTC_INICIAL AND DT_HR_UTC_FINAL OR
                   (DT_HR_UTC_INICIAL BETWEEN TO_DATE(:dtStart, 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE(:dtEnd, 'YYYY-MM-DD HH24:MI:SS') AND
                    DT_HR_UTC_FINAL BETWEEN TO_DATE(:dtStart, 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE(:dtEnd, 'YYYY-MM-DD HH24:MI:SS'))
                )
          ) select * from insideTalhao
          union all
          select glot.* from
            GEO_LAYER_OPERACAO_TALHAO glot
            , insideTalhao it
          where it.CD_MAPA_VAR = glot.CD_MAPA_VAR
          AND it.CD_UNIDADE = 0
          AND it.CD_FAZENDA = 0
          AND it.CD_ZONA = 0
          AND it.CD_TALHAO = 0
        ) t WHERE t.GEOMETRIA IS NOT NULL`
        , parameters : {
            cdCliente:      { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_cliente  }
            , cdUnidade:    { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_unidade  }
            , cdFazenda:    { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_fazenda  }
            , cdZona:       { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_zona     }
            , cdTalhao:     { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_talhao   }
            , cdOperacao:   { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_operacao }
            , dtStart:      { dir: oracle.BIND_IN, type: oracle.STRING, val: props.dt_start    }
            , dtEnd:        { dir: oracle.BIND_IN, type: oracle.STRING, val: props.dt_end      }
        }
    })
    , mapa_analitico_linhas_1fazenda_1operacao : (props) => ({
        query: `SELECT JSON_OBJECT('type' VALUE 'FeatureCollection'
        , 'features' VALUE JSON_ARRAYAGG(
          JSON_OBJECT('type' VALUE 'Feature'
            , 'properties' VALUE JSON_OBJECT(
              'cdUnidade' VALUE CD_UNIDADE
              , 'cdFazenda' VALUE CD_FAZENDA
              , 'cdZona' VALUE CD_ZONA
              , 'cdTalhao' VALUE CD_TALHAO
              , 'dtHrUtcInicial' VALUE TO_EPOCH(DT_HR_UTC_INICIAL)
              , 'dtHrUtcFinal' VALUE TO_EPOCH(DT_HR_UTC_FINAL)
              , 'cdOperacao' VALUE CD_OPERACAO
              , 'cdOperacParada' VALUE CD_OPERAC_PARADA
              , 'fgTpEquipamento' VALUE FG_TP_EQUIPAMENTO
              , 'cdEquipamento' VALUE CD_EQUIPAMENTO
              , 'cdImplemento' VALUE CD_IMPLEMENTO
              , 'cdEquipe' VALUE CD_EQUIPE
              , 'cdOperador' VALUE CD_OPERADOR
              , 'cdEstado' VALUE CD_ESTADO
              , 'descOperacao' VALUE DESC_OPERACAO
              , 'descEquipamento' VALUE DESC_EQUIPAMENTO
              , 'descOperador' VALUE DESC_OPERADOR
              , 'descTipoBico' VALUE DESC_TIPO_BICO
              , 'descOperacParada' VALUE DESC_OPERAC_PARADA
              , 'fgFrenteTrabalho' VALUE FG_FRENTE_TRABALHO
              , 'cdTipoBico' VALUE CD_TIPO_BICO
              , 'vlTempoSegundos' VALUE VL_TEMPO_SEGUNDOS
              , 'vlArea' VALUE VL_AREA
              , 'vlDistanciaMetros' VALUE VL_DISTANCIA_METROS
              , 'vlVelocidade' VALUE VL_VELOCIDADE
              , 'vlVelocidadeVento' VALUE VL_VELOCIDADE_VENTO
              , 'vlTemperatura' VALUE VL_TEMPERATURA
              , 'vlUmidade' VALUE VL_UMIDADE
              , 'vlConsumoInstantaneo' VALUE VL_CONSUMO_INSTANTANEO
              , 'vlRpm' VALUE VL_RPM
              , 'vlTemperaturaMotor' VALUE VL_TEMPERATURA_MOTOR
              , 'vlOrdemServico' VALUE VL_ORDEM_SERVICO
              , 'vlAlarme' VALUE VL_ALARME
              , 'vlAlertaClima' VALUE VL_ALERTA_CLIMA
              , 'vlAlarmeClima' VALUE VL_ALARME_CLIMA
              , 'vlHectaresHora' VALUE VL_HECTARES_HORA
              , 'vlHorimetroInicial' VALUE VL_HORIMETRO_INICIAL
              , 'vlHorimetroFinal' VALUE VL_HORIMETRO_FINAL
              , 'vlLarguraImplemento' VALUE VL_LARGURA_IMPLEMENTO
              , 'vlRendimentoColheita' VALUE VL_RENDIMENTO_COLHEITA
              , 'vlUmidadeGraos' VALUE VL_UMIDADE_GRAOS
              , 'vlPontoOrvalho' VALUE VL_PONTO_ORVALHO
              , 'vlVazaoLitrosHa' VALUE VL_VAZAO_LITROS_HA
              , 'vlVazaoLitrosMin' VALUE VL_VAZAO_LITROS_MIN
              , 'vlDosagemTaxa1' VALUE VL_DOSAGEM_TAXA1
              , 'vlDosagemTaxa2' VALUE VL_DOSAGEM_TAXA2
              , 'vlDosagemTaxa3' VALUE VL_DOSAGEM_TAXA3
              , 'vlPressaoBomba' VALUE VL_PRESSAO_BOMBA
              ABSENT ON NULL )
              , 'geometry' VALUE MDSYS.SDO_UTIL.TO_GEOJSON(LINHA) FORMAT JSON RETURNING BLOB
            ) RETURNING BLOB
        ) RETURNING BLOB
      ) GEOJSON
      FROM (
       WITH insideTalhao as (
         SELECT *
         FROM GEO_LAYER_OPERACAO_TALHAO
         WHERE CD_CLIENTE = :cdCliente
           AND CD_UNIDADE = :cdUnidade
           AND CD_FAZENDA = :cdFazenda
           AND CD_OPERACAO_CB = :cdOperacao
           AND (TO_DATE(:dtStart, 'YYYY-MM-DD HH24:MI:SS') BETWEEN DT_HR_UTC_INICIAL AND DT_HR_UTC_FINAL OR
                TO_DATE(:dtEnd, 'YYYY-MM-DD HH24:MI:SS') BETWEEN DT_HR_UTC_INICIAL AND DT_HR_UTC_FINAL OR
                (DT_HR_UTC_INICIAL BETWEEN TO_DATE(:dtStart, 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE(:dtEnd, 'YYYY-MM-DD HH24:MI:SS') AND
                 DT_HR_UTC_FINAL BETWEEN TO_DATE(:dtStart, 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE(:dtEnd, 'YYYY-MM-DD HH24:MI:SS'))
           )
         ) select * from insideTalhao
          union all
          select glot.* from
            GEO_LAYER_OPERACAO_TALHAO glot
            , insideTalhao it
          where it.CD_MAPA_VAR = glot.CD_MAPA_VAR
          AND it.CD_UNIDADE = 0
          AND it.CD_FAZENDA = 0
          AND it.CD_ZONA = 0
          AND it.CD_TALHAO = 0
      ) t WHERE t.LINHA IS NOT NULL`
      , parameters : {
            cdCliente:      { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_cliente  }
            , cdUnidade:    { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_unidade  }
            , cdFazenda:    { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_fazenda  }
            , cdZona:       { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_zona     }
            , cdOperacao:   { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_operacao }
            , dtStart:      { dir: oracle.BIND_IN, type: oracle.STRING, val: props.dt_start    }
            , dtEnd:        { dir: oracle.BIND_IN, type: oracle.STRING, val: props.dt_end      }
        }
    })
    , mapa_analitico_sobreposicao_1fazenda_1operacao : (props) => ({
        query: `SELECT JSON_OBJECT('type' VALUE 'FeatureCollection'
        , 'features' VALUE JSON_ARRAYAGG(
          JSON_OBJECT('type' VALUE 'Feature'
            , 'properties' VALUE JSON_OBJECT(
              'cdUnidade' VALUE CD_UNIDADE
              , 'cdFazenda' VALUE CD_FAZENDA
              , 'cdZona' VALUE CD_ZONA
              , 'cdTalhao' VALUE CD_TALHAO
              , 'dtHrUtcInicial' VALUE TO_EPOCH(DT_HR_UTC_INICIAL)
              , 'dtHrUtcFinal' VALUE TO_EPOCH(DT_HR_UTC_FINAL)
              , 'cdOperacao' VALUE CD_OPERACAO
              , 'cdOperacParada' VALUE CD_OPERAC_PARADA
              , 'fgTpEquipamento' VALUE FG_TP_EQUIPAMENTO
              , 'cdEquipamento' VALUE CD_EQUIPAMENTO
              , 'cdImplemento' VALUE CD_IMPLEMENTO
              , 'cdEquipe' VALUE CD_EQUIPE
              , 'cdOperador' VALUE CD_OPERADOR
              , 'cdEstado' VALUE CD_ESTADO
              , 'descOperacao' VALUE DESC_OPERACAO
              , 'descEquipamento' VALUE DESC_EQUIPAMENTO
              , 'descOperador' VALUE DESC_OPERADOR
              , 'descTipoBico' VALUE DESC_TIPO_BICO
              , 'descOperacParada' VALUE DESC_OPERAC_PARADA
              , 'fgFrenteTrabalho' VALUE FG_FRENTE_TRABALHO
              , 'cdTipoBico' VALUE CD_TIPO_BICO
              , 'vlTempoSegundos' VALUE VL_TEMPO_SEGUNDOS
              , 'vlArea' VALUE VL_AREA
              , 'vlDistanciaMetros' VALUE VL_DISTANCIA_METROS
              , 'vlVelocidade' VALUE VL_VELOCIDADE
              , 'vlVelocidadeVento' VALUE VL_VELOCIDADE_VENTO
              , 'vlTemperatura' VALUE VL_TEMPERATURA
              , 'vlUmidade' VALUE VL_UMIDADE
              , 'vlConsumoInstantaneo' VALUE VL_CONSUMO_INSTANTANEO
              , 'vlRpm' VALUE VL_RPM
              , 'vlTemperaturaMotor' VALUE VL_TEMPERATURA_MOTOR
              , 'vlOrdemServico' VALUE VL_ORDEM_SERVICO
              , 'vlAlarme' VALUE VL_ALARME
              , 'vlAlertaClima' VALUE VL_ALERTA_CLIMA
              , 'vlAlarmeClima' VALUE VL_ALARME_CLIMA
              , 'vlHectaresHora' VALUE VL_HECTARES_HORA
              , 'vlHorimetroInicial' VALUE VL_HORIMETRO_INICIAL
              , 'vlHorimetroFinal' VALUE VL_HORIMETRO_FINAL
              , 'vlLarguraImplemento' VALUE VL_LARGURA_IMPLEMENTO
              , 'vlRendimentoColheita' VALUE VL_RENDIMENTO_COLHEITA
              , 'vlUmidadeGraos' VALUE VL_UMIDADE_GRAOS
              , 'vlPontoOrvalho' VALUE VL_PONTO_ORVALHO
              , 'vlVazaoLitrosHa' VALUE VL_VAZAO_LITROS_HA
              , 'vlVazaoLitrosMin' VALUE VL_VAZAO_LITROS_MIN
              , 'vlDosagemTaxa1' VALUE VL_DOSAGEM_TAXA1
              , 'vlDosagemTaxa2' VALUE VL_DOSAGEM_TAXA2
              , 'vlDosagemTaxa3' VALUE VL_DOSAGEM_TAXA3
              , 'vlPressaoBomba' VALUE VL_PRESSAO_BOMBA
              ABSENT ON NULL )
              , 'geometry' VALUE MDSYS.SDO_UTIL.TO_GEOJSON(GEOMETRIA) FORMAT JSON RETURNING BLOB
            ) RETURNING BLOB
        ) RETURNING BLOB
      ) GEOJSON
      FROM (
       WITH insideTalhao as (
         SELECT *
         FROM GEO_LAYER_OPERACAO_TALHAO
         WHERE CD_CLIENTE = :cdCliente
           AND CD_UNIDADE = :cdUnidade
           AND CD_FAZENDA = :cdFazenda
           AND CD_OPERACAO_CB = :cdOperacao
           AND (TO_DATE(:dtStart, 'YYYY-MM-DD HH24:MI:SS') BETWEEN DT_HR_UTC_INICIAL AND DT_HR_UTC_FINAL OR
                TO_DATE(:dtEnd, 'YYYY-MM-DD HH24:MI:SS') BETWEEN DT_HR_UTC_INICIAL AND DT_HR_UTC_FINAL OR
                (DT_HR_UTC_INICIAL BETWEEN TO_DATE(:dtStart, 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE(:dtEnd, 'YYYY-MM-DD HH24:MI:SS') AND
                 DT_HR_UTC_FINAL BETWEEN TO_DATE(:dtStart, 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE(:dtEnd, 'YYYY-MM-DD HH24:MI:SS'))
           )
         ) select * from insideTalhao
          union all
          select glot.* from
            GEO_LAYER_OPERACAO_TALHAO glot
            , insideTalhao it
          where it.CD_MAPA_VAR = glot.CD_MAPA_VAR
          AND it.CD_UNIDADE = 0
          AND it.CD_FAZENDA = 0
          AND it.CD_ZONA = 0
          AND it.CD_TALHAO = 0
      ) t WHERE t.GEOMETRIA IS NOT NULL`
      , parameters : {
            cdCliente:      { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_cliente  }
            , cdUnidade:    { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_unidade  }
            , cdFazenda:    { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_fazenda  }
            , cdZona:       { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_zona     }
            , cdOperacao:   { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_operacao }
            , dtStart:      { dir: oracle.BIND_IN, type: oracle.STRING, val: props.dt_start    }
            , dtEnd:        { dir: oracle.BIND_IN, type: oracle.STRING, val: props.dt_end      }
        }
    })
    , mapa_analitico_sobreposicao_1unidade_1operacao : (props) => ({
        query : `SELECT JSON_OBJECT('type' VALUE 'FeatureCollection'
        , 'features' VALUE JSON_ARRAYAGG(
          JSON_OBJECT('type' VALUE 'Feature'
            , 'properties' VALUE JSON_OBJECT(
              'cdUnidade' VALUE CD_UNIDADE
              , 'cdFazenda' VALUE CD_FAZENDA
              , 'cdZona' VALUE CD_ZONA
              , 'cdTalhao' VALUE CD_TALHAO
              , 'dtHrUtcInicial' VALUE TO_EPOCH(DT_HR_UTC_INICIAL)
              , 'dtHrUtcFinal' VALUE TO_EPOCH(DT_HR_UTC_FINAL)
              , 'cdOperacao' VALUE CD_OPERACAO
              , 'cdOperacParada' VALUE CD_OPERAC_PARADA
              , 'fgTpEquipamento' VALUE FG_TP_EQUIPAMENTO
              , 'cdEquipamento' VALUE CD_EQUIPAMENTO
              , 'cdImplemento' VALUE CD_IMPLEMENTO
              , 'cdEquipe' VALUE CD_EQUIPE
              , 'cdOperador' VALUE CD_OPERADOR
              , 'cdEstado' VALUE CD_ESTADO
              , 'descOperacao' VALUE DESC_OPERACAO
              , 'descEquipamento' VALUE DESC_EQUIPAMENTO
              , 'descOperador' VALUE DESC_OPERADOR
              , 'descTipoBico' VALUE DESC_TIPO_BICO
              , 'descOperacParada' VALUE DESC_OPERAC_PARADA
              , 'fgFrenteTrabalho' VALUE FG_FRENTE_TRABALHO
              , 'cdTipoBico' VALUE CD_TIPO_BICO
              , 'vlTempoSegundos' VALUE VL_TEMPO_SEGUNDOS
              , 'vlArea' VALUE VL_AREA
              , 'vlDistanciaMetros' VALUE VL_DISTANCIA_METROS
              , 'vlVelocidade' VALUE VL_VELOCIDADE
              , 'vlVelocidadeVento' VALUE VL_VELOCIDADE_VENTO
              , 'vlTemperatura' VALUE VL_TEMPERATURA
              , 'vlUmidade' VALUE VL_UMIDADE
              , 'vlConsumoInstantaneo' VALUE VL_CONSUMO_INSTANTANEO
              , 'vlRpm' VALUE VL_RPM
              , 'vlTemperaturaMotor' VALUE VL_TEMPERATURA_MOTOR
              , 'vlOrdemServico' VALUE VL_ORDEM_SERVICO
              , 'vlAlarme' VALUE VL_ALARME
              , 'vlAlertaClima' VALUE VL_ALERTA_CLIMA
              , 'vlAlarmeClima' VALUE VL_ALARME_CLIMA
              , 'vlHectaresHora' VALUE VL_HECTARES_HORA
              , 'vlHorimetroInicial' VALUE VL_HORIMETRO_INICIAL
              , 'vlHorimetroFinal' VALUE VL_HORIMETRO_FINAL
              , 'vlLarguraImplemento' VALUE VL_LARGURA_IMPLEMENTO
              , 'vlRendimentoColheita' VALUE VL_RENDIMENTO_COLHEITA
              , 'vlUmidadeGraos' VALUE VL_UMIDADE_GRAOS
              , 'vlPontoOrvalho' VALUE VL_PONTO_ORVALHO
              , 'vlVazaoLitrosHa' VALUE VL_VAZAO_LITROS_HA
              , 'vlVazaoLitrosMin' VALUE VL_VAZAO_LITROS_MIN
              , 'vlDosagemTaxa1' VALUE VL_DOSAGEM_TAXA1
              , 'vlDosagemTaxa2' VALUE VL_DOSAGEM_TAXA2
              , 'vlDosagemTaxa3' VALUE VL_DOSAGEM_TAXA3
              , 'vlPressaoBomba' VALUE VL_PRESSAO_BOMBA
              ABSENT ON NULL )
              , 'geometry' VALUE MDSYS.SDO_UTIL.TO_GEOJSON(GEOMETRIA) FORMAT JSON RETURNING BLOB
            ) RETURNING BLOB
        ) RETURNING BLOB
        ) GEOJSON
        FROM (
          with insideTalhao as (
            SELECT * FROM GEO_LAYER_OPERACAO_TALHAO
            WHERE CD_CLIENTE = :cdCliente
            AND CD_UNIDADE = :cdUnidade
            AND CD_OPERACAO_CB = :cdOperacao
          )
          select * from insideTalhao
          union all
          select glot.* from
            GEO_LAYER_OPERACAO_TALHAO glot
            , insideTalhao it
          where it.CD_MAPA_VAR = glot.CD_MAPA_VAR
          AND it.CD_UNIDADE = 0
          AND it.CD_FAZENDA = 0
          AND it.CD_ZONA = 0
          AND it.CD_TALHAO = 0
        ) t WHERE t.GEOMETRIA IS NOT NULL`
        , parameters : {
            cdCliente:      { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_cliente  }
            , cdUnidade:    { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_unidade  }
            , cdOperacao:   { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_operacao }
        }
    })
    , mapa_unidade : (props) => ({
        query : `SELECT JSON_OBJECT('type' VALUE 'FeatureCollection'
        , 'features' VALUE JSON_ARRAYAGG(
            JSON_OBJECT(
            'type' VALUE 'Feature'
            , 'id' VALUE ROWNUM
            , 'properties' VALUE JSON_OBJECT(
                'cdUnidade' VALUE CD_UNIDADE
                , 'cdFazenda' VALUE CD_FAZENDA
                , 'cdZona' VALUE CD_ZONA
                , 'cdTalhao' VALUE CD_TALHAO
                ABSENT ON NULL
            )
            , 'geometry' VALUE MDSYS.SDO_UTIL.TO_GEOJSON(GEOMETRIA) FORMAT JSON RETURNING BLOB
            ) RETURNING BLOB
        ) RETURNING BLOB
        ) GEOJSON
        FROM GEO_LAYER_TALHAO where CD_CLIENTE = :cdCliente AND CD_UNIDADE = NVL(:cdUnidade, CD_UNIDADE)`
        , parameters : {
            cdCliente:      { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_cliente  }
            , cdUnidade:    { dir: oracle.BIND_IN, type: oracle.NUMBER, val: props.cd_unidade  }
        }
    })
  
}

function generateProfile(profile_name, operation_name, args){
    operation_name = operation_name || profile_name
    return Object.assign({}, profiles[profile_name](args), profiles[operation_name](args));
}

module.exports = generateProfile