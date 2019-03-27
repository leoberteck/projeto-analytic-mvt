CREATE OR REPLACE VIEW VW_ANALYTIC_LINES AS
SELECT
  dz_json_feature(
      p_geometry =>  SDO_GEOMETRY(
        2002,
        8265,
        NULL,
        SDO_ELEM_INFO_ARRAY(1,2,1),
        SDO_ORDINATE_ARRAY(VL_LONGITUDE_INICIAL,VL_LATITUDE_INICIAL,VL_LONGITUDE_FINAL, VL_LATITUDE_FINAL)
      )
      , p_properties => dz_json_properties_vry(
          dz_json_properties(
              p_name => 'id'
             ,p_properties_number => ROWNUM
          )
          , dz_json_properties(
              p_name => 'cdUnidade'
             ,p_properties_number => CD_UNIDADE
          )
          , dz_json_properties(
              p_name => 'cdFazendaInicial'
             ,p_properties_string => CD_FAZENDA_INICIAL
          )
          , dz_json_properties(
              p_name => 'cdZonaInicial'
             ,p_properties_string => CD_ZONA_INICIAL
          )
          , dz_json_properties(
              p_name => 'cdTalhaoInicial'
             ,p_properties_string => CD_TALHAO_INICIAL
          )
          , dz_json_properties(
              p_name => 'cdFazendaFinal'
             ,p_properties_string => CD_FAZENDA_FINAL
          )
          , dz_json_properties(
              p_name => 'cdZonaFinal'
             ,p_properties_string => CD_ZONA_FINAL
          )
          , dz_json_properties(
              p_name => 'cdTalhaoFinal'
             ,p_properties_string => CD_TALHAO_FINAL
          )
          , dz_json_properties(
              p_name => 'cdEquipamento'
             ,p_properties_string => CD_EQUIPAMENTO
          )
          , dz_json_properties(
              p_name => 'descEquipamento'
             ,p_properties_string => DESC_EQUIPAMENTO
          )
          , dz_json_properties(
              p_name => 'VL_LONGITUDE_INICIAL'
             ,p_properties_number => VL_LONGITUDE_INICIAL
          )
          , dz_json_properties(
              p_name => 'VL_LATITUDE_INICIAL'
             ,p_properties_number => VL_LATITUDE_INICIAL
          )
          , dz_json_properties(
              p_name => 'VL_LONGITUDE_FINAL'
             ,p_properties_number => VL_LONGITUDE_FINAL
          )
          , dz_json_properties(
              p_name => 'VL_LATITUDE_FINAL'
             ,p_properties_number => VL_LATITUDE_FINAL
          )
          , dz_json_properties(
              p_name => 'start'
             ,p_properties_number => TO_EPOCH(DT_HR_UTC_INICIAL)
          )
          , dz_json_properties(
              p_name => 'end'
             ,p_properties_number => TO_EPOCH(DT_HR_UTC_FINAL)
          )
          , dz_json_properties(
              p_name => 'cdOperacao'
             ,p_properties_number => CD_OPERACAO
          )
          , dz_json_properties(
              p_name => 'descOperacao'
             ,p_properties_string => DESC_OPERACAO
          )
          , dz_json_properties(
              p_name => 'cdOperador'
             ,p_properties_number => CD_OPERADOR
          )
          , dz_json_properties(
              p_name => 'descOperador'
             ,p_properties_string => DESC_OPERADOR
          )
          , dz_json_properties(
              p_name => 'vlTempoSegundos'
             ,p_properties_number => VL_TEMPO_SEGUNDOS
          )
          , dz_json_properties(
              p_name => 'vlDistanciaMetros'
             ,p_properties_number => VL_DISTANCIA_METROS
          )
          , dz_json_properties(
              p_name => 'cdEstado'
             ,p_properties_string => CD_ESTADO
          )
          , dz_json_properties(
              p_name => 'cdOperacParada'
             ,p_properties_number => CD_OPERAC_PARADA
          )
          , dz_json_properties(
              p_name => 'descOperacParada'
             ,p_properties_string => DESC_OPERAC_PARADA
          )
          , dz_json_properties(
              p_name => 'vlOrdemServico'
             ,p_properties_number => VL_ORDEM_SERVICO
          )
          , dz_json_properties(
              p_name => 'vlVelocidade'
             ,p_properties_number => VL_VELOCIDADE
          )
          , dz_json_properties(
              p_name => 'vlRpm'
             ,p_properties_number => VL_RPM
          )
          , dz_json_properties(
              p_name => 'vlTemperaturaMotor'
             ,p_properties_number => VL_TEMPERATURA_MOTOR
          )
          , dz_json_properties(
              p_name => 'vlPressaoBomba'
             ,p_properties_number => VL_PRESSAO_BOMBA
          )
          , dz_json_properties(
              p_name => 'vlParticulasOleo'
             ,p_properties_string => VL_PARTICULAS_OLEO
          )
          , dz_json_properties(
              p_name => 'vlVelocidadeVento'
             ,p_properties_number => VL_VELOCIDADE_VENTO
          )
          , dz_json_properties(
              p_name => 'vlUmidade'
             ,p_properties_number => VL_UMIDADE
          )
          , dz_json_properties(
              p_name => 'vlHectaresHora'
             ,p_properties_number => VL_HECTARES_HORA
          )
          , dz_json_properties(
              p_name => 'vlTemperatura'
             ,p_properties_number => VL_TEMPERATURA
          )
          , dz_json_properties(
              p_name => 'vlPontoOrvalho'
             ,p_properties_number => VL_PONTO_ORVALHO
          )
          , dz_json_properties(
              p_name => 'vlRendimentoColheita'
             ,p_properties_number => VL_RENDIMENTO_COLHEITA
          )
        )
      ).toJSON(
          p_2d_flag       => 'FALSE'
         ,p_pretty_print  => 0
         ,p_prune_number  => 8
         ,p_add_bbox      => 'FALSE'
      ) feature
FROM GEO_LAYER_MAPA_VAR;
/
CREATE OR REPLACE VIEW VW_ANALYTIC_POLYGONS AS
WITH related as (
  SELECT * FROM (
    SELECT ROWID
      , SDO_GEOM.RELATE(
        NVL(VL_GEOM_INTERS_POLIGTALHAOINI, VL_GEOM_INTERSPOLIGTALHINI_M)
        , 'determine'
        , NVL(VL_GEOM_INTERS_POLIGTALHAOFIM, VL_GEOM_INTERSPOLIGTALHFIM_M)
        , 0.005
      ) relationship
    FROM GEO_LAYER_MAPA_VAR
  ) WHERE relationship IS NOT NULL)
, differ as (select * from related where relationship <> 'EQUAL')
, result as (
SELECT glmv.CD_EQUIPAMENTO
  , glmv.DESC_EQUIPAMENTO
  , TO_EPOCH(glmv.DT_HR_UTC_INICIAL) "START"
  , TO_EPOCH(glmv.DT_HR_UTC_FINAL) "END"
  , glmv.CD_UNIDADE
  , glmv.CD_FAZENDA_INICIAL CD_FAZENDA
  , glmv.CD_ZONA_INICIAL CD_ZONA
  , glmv.CD_TALHAO_INICIAL CD_TALHAO
  , glmv.CD_OPERACAO
  , glmv.DESC_OPERACAO
  , glmv.CD_OPERADOR
  , glmv.DESC_OPERADOR
  , glmv.VL_TEMPO_SEGUNDOS
  , glmv.VL_DISTANCIA_METROS
  , glmv.CD_ESTADO
  , glmv.CD_OPERAC_PARADA
  , glmv.DESC_OPERAC_PARADA
  , glmv.VL_ORDEM_SERVICO
  , glmv.VL_VELOCIDADE
  , glmv.VL_RPM
  , glmv.VL_TEMPERATURA_MOTOR
  , glmv.VL_PRESSAO_BOMBA
  , glmv.VL_PARTICULAS_OLEO
  , glmv.VL_VELOCIDADE_VENTO
  , glmv.VL_UMIDADE
  , glmv.VL_HECTARES_HORA
  , glmv.VL_TEMPERATURA
  , glmv.VL_PONTO_ORVALHO
  , glmv.VL_RENDIMENTO_COLHEITA
  , NVL(glmv.VL_GEOM_INTERS_POLIGTALHAOINI, glmv.VL_GEOM_INTERSPOLIGTALHINI_M) POLIGONO
FROM related
  JOIN GEO_LAYER_MAPA_VAR glmv ON glmv.ROWID = related.ROWID
UNION ALL
SELECT glmv.CD_EQUIPAMENTO
  , glmv.DESC_EQUIPAMENTO
  , TO_EPOCH(glmv.DT_HR_UTC_INICIAL) "START"
  , TO_EPOCH(glmv.DT_HR_UTC_FINAL) "END"
  , glmv.CD_UNIDADE
  , glmv.CD_FAZENDA_FINAL CD_FAZENDA
  , glmv.CD_ZONA_FINAL CD_ZONA
  , glmv.CD_TALHAO_FINAL CD_TALHAO
  , glmv.CD_OPERACAO
  , glmv.DESC_OPERACAO
  , glmv.CD_OPERADOR
  , glmv.DESC_OPERADOR
  , glmv.VL_TEMPO_SEGUNDOS
  , glmv.VL_DISTANCIA_METROS
  , glmv.CD_ESTADO
  , glmv.CD_OPERAC_PARADA
  , glmv.DESC_OPERAC_PARADA
  , glmv.VL_ORDEM_SERVICO
  , glmv.VL_VELOCIDADE
  , glmv.VL_RPM
  , glmv.VL_TEMPERATURA_MOTOR
  , glmv.VL_PRESSAO_BOMBA
  , glmv.VL_PARTICULAS_OLEO
  , glmv.VL_VELOCIDADE_VENTO
  , glmv.VL_UMIDADE
  , glmv.VL_HECTARES_HORA
  , glmv.VL_TEMPERATURA
  , glmv.VL_PONTO_ORVALHO
  , glmv.VL_RENDIMENTO_COLHEITA
  , NVL(glmv.VL_GEOM_INTERS_POLIGTALHAOFIM, glmv.VL_GEOM_INTERSPOLIGTALHFIM_M) POLIGONO
FROM differ
  JOIN GEO_LAYER_MAPA_VAR glmv ON glmv.ROWID = differ.ROWID)
SELECT
  dz_json_feature(
    p_geometry => POLIGONO
    , p_properties => dz_json_properties_vry(
        dz_json_properties(
            p_name => 'id'
           ,p_properties_number => ROWNUM
        )
        , dz_json_properties(
            p_name => 'cdUnidade'
           ,p_properties_number => CD_UNIDADE
        )
        , dz_json_properties(
            p_name => 'cdFazenda'
           ,p_properties_string => CD_FAZENDA
        )
        , dz_json_properties(
            p_name => 'cdZona'
           ,p_properties_string => CD_ZONA
        )
        , dz_json_properties(
            p_name => 'cdTalhao'
           ,p_properties_string => CD_TALHAO
        )
        , dz_json_properties(
            p_name => 'cdEquipamento'
           ,p_properties_string => CD_EQUIPAMENTO
        )
        , dz_json_properties(
            p_name => 'descEquipamento'
           ,p_properties_string => DESC_EQUIPAMENTO
        )
        , dz_json_properties(
            p_name => 'start'
           ,p_properties_number => "START"
        )
        , dz_json_properties(
            p_name => 'end'
           ,p_properties_number => "END"
        )
        , dz_json_properties(
            p_name => 'cdOperacao'
           ,p_properties_number => CD_OPERACAO
        )
        , dz_json_properties(
            p_name => 'descOperacao'
           ,p_properties_string => DESC_OPERACAO
        )
        , dz_json_properties(
            p_name => 'cdOperador'
           ,p_properties_number => CD_OPERADOR
        )
        , dz_json_properties(
            p_name => 'descOperador'
           ,p_properties_string => DESC_OPERADOR
        )
        , dz_json_properties(
            p_name => 'vlTempoSegundos'
           ,p_properties_number => VL_TEMPO_SEGUNDOS
        )
        , dz_json_properties(
            p_name => 'vlDistanciaMetros'
           ,p_properties_number => VL_DISTANCIA_METROS
        )
        , dz_json_properties(
            p_name => 'cdEstado'
           ,p_properties_string => CD_ESTADO
        )
        , dz_json_properties(
            p_name => 'cdOperacParada'
           ,p_properties_number => CD_OPERAC_PARADA
        )
        , dz_json_properties(
            p_name => 'descOperacParada'
           ,p_properties_string => DESC_OPERAC_PARADA
        )
        , dz_json_properties(
            p_name => 'vlOrdemServico'
           ,p_properties_number => VL_ORDEM_SERVICO
        )
        , dz_json_properties(
            p_name => 'vlVelocidade'
           ,p_properties_number => VL_VELOCIDADE
        )
        , dz_json_properties(
            p_name => 'vlRpm'
           ,p_properties_number => VL_RPM
        )
        , dz_json_properties(
            p_name => 'vlTemperaturaMotor'
           ,p_properties_number => VL_TEMPERATURA_MOTOR
        )
        , dz_json_properties(
            p_name => 'vlPressaoBomba'
           ,p_properties_number => VL_PRESSAO_BOMBA
        )
        , dz_json_properties(
            p_name => 'vlParticulasOleo'
           ,p_properties_string => VL_PARTICULAS_OLEO
        )
        , dz_json_properties(
            p_name => 'vlVelocidadeVento'
           ,p_properties_number => VL_VELOCIDADE_VENTO
        )
        , dz_json_properties(
            p_name => 'vlUmidade'
           ,p_properties_number => VL_UMIDADE
        )
        , dz_json_properties(
            p_name => 'vlHectaresHora'
           ,p_properties_number => VL_HECTARES_HORA
        )
        , dz_json_properties(
            p_name => 'vlTemperatura'
           ,p_properties_number => VL_TEMPERATURA
        )
        , dz_json_properties(
            p_name => 'vlPontoOrvalho'
           ,p_properties_number => VL_PONTO_ORVALHO
        )
        , dz_json_properties(
            p_name => 'vlRendimentoColheita'
           ,p_properties_number => VL_RENDIMENTO_COLHEITA
        )
      )
    ).toJSON(
        p_2d_flag       => 'FALSE'
       ,p_pretty_print  => 0
       ,p_prune_number  => 8
       ,p_add_bbox      => 'FALSE'
    ) feature
FROM result;

SELECT
dz_json_feature(
    p_geometry => MDSYS.SDO_GEOMETRY(
         3001
        ,8265
        ,MDSYS.SDO_POINT_TYPE(-65.12345678901234567890,41.12345678901234567890,88.12345678901234567890)
        ,NULL
        ,NULL
    )
   , p_properties => dz_json_properties_vry(
        dz_json_properties(
            p_name => 'FeatureID'
           ,p_properties_number => 1234
        )
       ,dz_json_properties(
            p_name => 'FriedChickenStyle'
           ,p_properties_string => 'Southern'
        )
       ,dz_json_properties(
            p_name => 'AverageCostOfLemonade'
           ,p_properties_number => 4.32
        )
    )
).toJSON(
    p_2d_flag       => 'FALSE'
   ,p_pretty_print  => 0
   ,p_prune_number  => 8
   ,p_add_bbox      => 'FALSE'
)
FROM dual;
