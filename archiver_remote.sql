create or replace PACKAGE tw_archiver
AS
-- Generic archiver for DLCM managed tables
-- Supports daily and monthly range partitioned or range/list subpartitioned tables that are already being managed by DLCM
--
-- Ver Date Author Remarks

-- Identify new partitions created by DLCM on TW, load their details into the metadata and create corresponding partitions on the archive DB
PROCEDURE identify_partitions;

-- Load partitions which meet the aging criteria from TW into the archive database, marking them as archived when successful
PROCEDURE import_partitions;

-- Purge partitions which have gone past their retention period from the database, exporting them to tape first if specified
PROCEDURE purge_partitions;
PROCEDURE purge_monthly_partitions;

-- Procedure to count rows in operational and archive DB in each partition as a data quality check
PROCEDURE check_data_quality;

-- Procedure to reload any partitions which have previously failed the data quality check
PROCEDURE reload_partitions;

-- temporarily exposed for testing
PROCEDURE load_range_partition(p_owner IN VARCHAR2, p_table_name IN VARCHAR2, p_low_date IN DATE, p_high_date IN DATE, p_partition_name IN VARCHAR2, p_subpartition_key IN VARCHAR2);

END tw_archiver;
/

create or replace PACKAGE BODY tw_archiver
AS
-- 4.8 17-08-2024 Tomasz Lesinski Add support for 'INF', 'RN' for GAI_BAL_TAB
g_package CONSTANT VARCHAR2(30) := 'TW_ARCHIVER';
g_trace CONSTANT BOOLEAN := TRUE;
-- TODO: Set up environment to automatically set this constant at compile time based on the target environment
g_tw_link CONSTANT VARCHAR2(30) := 'TWP';

-- Private prototypes
--PROCEDURE load_range_partition(p_owner IN VARCHAR2, p_table_name IN VARCHAR2, p_low_date IN DATE, p_high_date IN DATE, p_partition_name IN VARCHAR2, p_subpartition_key IN VARCHAR2);
FUNCTION execute_stmt(p_stmt IN VARCHAR2) RETURN PLS_INTEGER;
PROCEDURE export_partition(p_owner IN VARCHAR2, p_table_name IN VARCHAR2, p_partition_name IN VARCHAR2);
PROCEDURE span_info(p_package IN VARCHAR2, p_procname IN VARCHAR2, p_message IN VARCHAR2);
--PROCEDURE purge_monthly_partitions;
PROCEDURE exchange_partition(p_owner IN VARCHAR2, p_table_name IN VARCHAR2, p_partition_name IN VARCHAR2, p_tmp_table_name IN VARCHAR2, p_subpartitioning_type IN VARCHAR2);
PROCEDURE rebuild_indexes(p_owner IN VARCHAR2, p_table_name IN VARCHAR2, p_partition_name IN VARCHAR2, p_subpartitioning_type IN VARCHAR2);
PROCEDURE shrink_monthly_partition( p_table_owner IN VARCHAR2, p_table_name IN VARCHAR2, p_partition_name IN VARCHAR2, p_tablespace_name IN VARCHAR2
    , p_partition_key IN VARCHAR2, p_partition_value IN DATE, p_subpartitioning_type IN VARCHAR2, p_subpartition_key IN VARCHAR2, p_subpartition_value IN VARCHAR2);

-- Public interface
PROCEDURE reload_partitions
IS
    c_jobname CONSTANT VARCHAR2(30) := 'TWAR_RELOAD_PARTITIONS';
    c_procname CONSTANT VARCHAR2(30) := 'RELOAD_PARTITIONS';
    c_prod_date CONSTANT DATE := dat.prod_date;
    v_job_id NUMBER;
    v_subpartition_key VARCHAR2(4000);
BEGIN
    v_job_id := JobLogger.jobStarting(c_jobname, c_prod_date);
    Logger.logEntry(g_package, c_procname, NULL);

    span_info(g_package, c_procname, 'Reloading partitions that have failed the data quality check');
    FOR p IN (
        SELECT p.table_owner, p.table_name, p.partition_name, p.prev_high_date, p.high_date
        FROM tw_archive_partitions p
        WHERE p.partition_archived='Y' AND p.partition_removed='N'
        AND p.archive_db_rows != p.ops_db_rows
    ) LOOP
        load_range_partition( p.table_owner, p.table_name, p.prev_high_date, p.high_date, p.partition_name, NULL);
        span_info(g_package, c_procname, 'Marking '||p.table_owner||'.'||p.table_name||' ( '||p.partition_name||') as reloaded');
        UPDATE tw_archive_partitions q
        SET archive_db_rows = NULL, ops_db_rows = NULL
        WHERE p.table_owner = q.table_owner AND p.table_name = q.table_name AND p.partition_name = q.partition_name;
        COMMIT;
    END LOOP;

    span_info(g_package, c_procname, 'Reloading subpartitions that have failed the data quality check');
    FOR p IN (
        SELECT p.table_owner, p.table_name, p.partition_name, p.prev_high_date, p.high_date, s.subpartition_name, x.high_value
        FROM tw_archive_partitions p
        JOIN tw_archive_subpartitions s ON p.table_owner = s.table_owner AND p.table_name = s.table_name AND p.partition_name = s.partition_name
        JOIN dba_tab_subpartitions x ON p.table_owner = x.table_owner AND p.table_name = x.table_name
            AND p.partition_name = x.partition_name AND s.subpartition_name = x.subpartition_name
        WHERE s.partition_archived='Y' AND p.partition_removed='N'
        AND s.archive_db_rows != s.ops_db_rows
    ) LOOP
        EXECUTE IMMEDIATE 'BEGIN :b1 := '||p.high_value||'; END;' USING OUT v_subpartition_key;

        load_range_partition( p.table_owner, p.table_name, p.prev_high_date, p.high_date, p.subpartition_name, v_subpartition_key);
        span_info(g_package, c_procname, 'Marking '||p.table_owner||'.'||p.table_name||' ( '||p.partition_name||':'||v_subpartition_key||') as reloaded');
        UPDATE tw_archive_subpartitions q
        SET archive_db_rows = NULL, ops_db_rows = NULL
        WHERE p.table_owner = q.table_owner AND p.table_name = q.table_name AND p.partition_name = q.partition_name and p.subpartition_name = q.subpartition_name;
        COMMIT;
    END LOOP;

    Logger.logExit(g_package, c_procname, NULL);
    JobLogger.jobEnding(c_jobname, 'Main', 0);
EXCEPTION
    WHEN OTHERS THEN
        Logger.logError(g_package, c_procname, SQLERRM);
        JobLogger.jobEnding(c_jobname, 'Exception ORA'||SQLCODE, 0);
        ROLLBACK;
        RAISE;
END reload_partitions;

PROCEDURE check_data_quality
IS
    c_jobname CONSTANT VARCHAR2(30) := 'TWAR_CHECK_DATA_QUALITY';
    c_procname CONSTANT VARCHAR2(30) := 'CHECK_DATA_QUALITY';
    c_prod_date CONSTANT DATE := dat.prod_date;
    c_archive_degree CONSTANT NUMBER := 12;
    c_ops_degree CONSTANT NUMBER := 32;
    v_stmt VARCHAR2(4000);
    v_job_id NUMBER;
    v_archive_cnt NUMBER;
    v_ops_cnt NUMBER;
BEGIN
    v_job_id := JobLogger.jobStarting(c_jobname, c_prod_date);
    Logger.logEntry(g_package, c_procname, NULL);

    -- Derive missing/wrong counts for partitioned tables
    FOR p IN (
        SELECT p.table_owner, p.table_name, p.partition_name, c.column_name, p.high_date, p.prev_high_date
        FROM tw_archive_partitions p
        JOIN dba_part_key_columns c ON c.owner = p.table_owner AND c.name = p.table_name AND c.object_type = 'TABLE'
        WHERE p.partition_archived = 'Y' AND p.partition_removed = 'N'
        AND (p.archive_db_rows IS NULL OR p.ops_db_rows IS NULL OR p.archive_db_rows != p.ops_db_rows)
  AND table_name not IN ('GAI_BAL_TAB')
        ORDER BY p.table_owner, p.table_name, p.high_date
    ) LOOP
        v_stmt := 'SELECT /*+ PARALLEL(t '||c_archive_degree||') FULL(t) */ COUNT(*) FROM "'||p.table_owner||'"."'||p.table_name||'" t WHERE "'||p.column_name
            ||'" >= TO_DATE('''||TO_CHAR(p.prev_high_date, 'DD-MON-YYYY')||''', ''DD-MON-YYYY'') AND "'||p.column_name
            ||'" < TO_DATE('''||TO_CHAR(p.high_date, 'DD-MON-YYYY')||''', ''DD-MON-YYYY'')';
        span_info(g_package, c_procname, v_stmt);
        EXECUTE IMMEDIATE v_stmt INTO v_archive_cnt;
        span_info(g_package, c_procname, 'Counted rows '||v_archive_cnt);

        v_stmt := 'SELECT /*+ PARALLEL(t '||c_ops_degree||') FULL(t) */ COUNT(*) FROM "'||p.table_owner||'"."'||p.table_name||'"@'||g_tw_link||' t WHERE "'||p.column_name
            ||'" >= TO_DATE('''||TO_CHAR(p.prev_high_date, 'DD-MON-YYYY')||''', ''DD-MON-YYYY'') AND "'||p.column_name
            ||'" < TO_DATE('''||TO_CHAR(p.high_date, 'DD-MON-YYYY')||''', ''DD-MON-YYYY'')';
        span_info(g_package, c_procname, v_stmt);
        EXECUTE IMMEDIATE v_stmt INTO v_ops_cnt;
        span_info(g_package, c_procname, 'Counted rows '||v_archive_cnt);

        UPDATE tw_archive_partitions SET ops_db_rows = v_ops_cnt, archive_db_rows = v_archive_cnt
        WHERE table_owner = p.table_owner AND table_name = p.table_name AND partition_name = p.partition_name;
    END LOOP;

    -- Derive missing/wrong counts for subpartitioned tables
    FOR p IN (
        SELECT p.table_owner, p.table_name, p.partition_name, c.column_name, p.high_date, p.prev_high_date
            , s.subpartition_name, d.column_name subpart_col_name, x.high_value
        FROM tw_archive_partitions p
        JOIN tw_archive_subpartitions s ON s.table_owner = p.table_owner AND s.table_name = p.table_name AND s.partition_name = p.partition_name
        JOIN dba_tab_subpartitions x ON s.table_owner = x.table_owner AND s.table_name = x.table_name
            AND s.partition_name = x.partition_name AND s.subpartition_name = x.subpartition_name
        JOIN dba_part_key_columns c ON c.owner = p.table_owner AND c.name = p.table_name AND c.object_type = 'TABLE'
        JOIN dba_subpart_key_columns d ON d.owner = p.table_owner AND d.name = p.table_name AND d.object_type = 'TABLE'
        WHERE s.partition_archived = 'Y' AND p.partition_removed = 'N'
        AND (s.archive_db_rows IS NULL OR s.ops_db_rows IS NULL OR s.archive_db_rows != s.ops_db_rows)
        ORDER BY p.table_owner, p.table_name, p.high_date, x.subpartition_position
    ) LOOP
        v_stmt := 'SELECT /*+ PARALLEL(t '||c_archive_degree||') FULL(t) */ COUNT(*) FROM "'||p.table_owner||'"."'||p.table_name||'" t WHERE "'||p.column_name
            ||'" >= TO_DATE('''||TO_CHAR(p.prev_high_date, 'DD-MON-YYYY')||''', ''DD-MON-YYYY'') AND "'||p.column_name
            ||'" < TO_DATE('''||TO_CHAR(p.high_date, 'DD-MON-YYYY')||''', ''DD-MON-YYYY'') AND "'||p.subpart_col_name||'" = '||p.high_value;
        span_info(g_package, c_procname, v_stmt);
        EXECUTE IMMEDIATE v_stmt INTO v_archive_cnt;
        span_info(g_package, c_procname, 'Counted rows '||v_archive_cnt);

        v_stmt := 'SELECT /*+ PARALLEL(t '||c_ops_degree||') FULL(t) */ COUNT(*) FROM "'||p.table_owner||'"."'||p.table_name||'"@'||g_tw_link||' t WHERE "'||p.column_name
            ||'" >= TO_DATE('''||TO_CHAR(p.prev_high_date, 'DD-MON-YYYY')||''', ''DD-MON-YYYY'') AND "'||p.column_name
            ||'" < TO_DATE('''||TO_CHAR(p.high_date, 'DD-MON-YYYY')||''', ''DD-MON-YYYY'') AND "'||p.subpart_col_name||'" = '||p.high_value;
        span_info(g_package, c_procname, v_stmt);
        EXECUTE IMMEDIATE v_stmt INTO v_ops_cnt;
        span_info(g_package, c_procname, 'Counted rows '||v_ops_cnt);

        UPDATE tw_archive_subpartitions SET ops_db_rows = v_ops_cnt, archive_db_rows = v_archive_cnt
        WHERE table_owner = p.table_owner AND table_name = p.table_name AND partition_name = p.partition_name AND subpartition_name = p.subpartition_name;
    END LOOP;

    COMMIT;

    Logger.logExit(g_package, c_procname, NULL);
    JobLogger.jobEnding(c_jobname, 'Main', 0);
EXCEPTION
    WHEN OTHERS THEN
        Logger.logError(g_package, c_procname, SQLERRM);
        JobLogger.jobEnding(c_jobname, 'Exception ORA'||SQLCODE, 0);
        ROLLBACK;
        RAISE;
END check_data_quality;

PROCEDURE identify_partitions
IS
    c_jobname CONSTANT VARCHAR2(30) := 'TWAR_IDENTIFY_PARTITIONS';
    c_prod_date CONSTANT DATE := dat.prod_date;
    c_procname CONSTANT VARCHAR2(30) := 'IDENTIFY_PARTITIONS';
    v_stmt VARCHAR2(4000);
    v_prev_high_date DATE := NULL;
    v_high_date DATE;
    v_job_id NUMBER;
    --TL 20240817
    l_loc_subpart_name VARCHAR2(128);
    l_rmt_subpart_name VARCHAR2(128);
BEGIN
    v_job_id := JobLogger.jobStarting(c_jobname, c_prod_date);

    Logger.logEntry(g_package, c_procname, NULL);

    -- Don't create segments until we need them
    v_stmt := 'ALTER SESSION SET deferred_segment_creation = TRUE';
    span_info(g_package, c_procname, v_stmt);
    EXECUTE IMMEDIATE v_stmt;

    FOR ptn IN (
        SELECT v.table_owner, v.table_name, v.partition_name, v.partition_position, v.dict_high_date, v.dict_prev_high_date, v.subpartition_count
        FROM tw_identify_partitions_vw v
        WHERE v.meta_high_date IS NULL
    ) 
    LOOP
        -- Create an archive partition with the same name and high bound as the new one created by DLCM on ops DB
        -- We are relying on the subpartition template here where the target table is subpartitioned
        v_stmt := 'ALTER TABLE "'||ptn.table_owner||'"."'||ptn.table_name||'" ADD PARTITION "'||ptn.partition_name
            ||'" VALUES LESS THAN (TO_DATE('''||TO_CHAR(ptn.dict_high_date, 'DD-MON-YYYY')||''', ''DD-MON-YYYY''))';
        span_info(g_package, c_procname, v_stmt);
        EXECUTE IMMEDIATE v_stmt;

        --TL 20240817
        --modify subpartition names for GAI_BAL_TAB for _RN if needed
        IF ptn.table_owner='TGL' AND ptn.table_name='GAI_BAL_TAB' THEN
            --get local subpartition_name
            v_stmt := 'SELECT subpartition_name 
              FROM dba_tab_subpartitions
             WHERE table_owner='''||ptn.table_owner||'''
               AND table_name='''||ptn.table_name||'''
               AND partition_name='''||ptn.partition_name||'''
               AND subpartition_name LIKE ''%\_RN'' escape ''\''';

            span_info(g_package, c_procname, v_stmt);
            EXECUTE IMMEDIATE v_stmt INTO l_loc_subpart_name; 

            --get remote subpartition_name
            v_stmt := 'SELECT subpartition_name 
              FROM dba_tab_subpartitions@'||g_tw_link||'
             WHERE table_owner='''||ptn.table_owner||'''
               AND table_name='''||ptn.table_name||'''
               AND partition_name='''||ptn.partition_name||'''
               AND subpartition_name LIKE ''%\_RN'' escape ''\''';

            span_info(g_package, c_procname, v_stmt);

            EXECUTE IMMEDIATE v_stmt INTO l_rmt_subpart_name; 

            span_info(g_package, c_procname, 'Local: ' || l_loc_subpart_name || ' Remote: ' || l_rmt_subpart_name);

            IF l_loc_subpart_name != l_rmt_subpart_name THEN
                v_stmt := 'ALTER TABLE "'||ptn.table_owner||'"."'||ptn.table_name||'"'|| CHR(10) ||
                         'RENAME SUBPARTITION '||l_loc_subpart_name|| CHR(10) ||
                         'TO '||l_rmt_subpart_name;

                span_info(g_package, c_procname, v_stmt);

                EXECUTE IMMEDIATE v_stmt;
            END IF;
        END IF;

        -- Create metadata for the new TWARP partition and subpartitions if applicable
        INSERT INTO tw_archive_partitions
        ( table_owner, table_name, partition_name, partition_archived, partition_removed, high_date, prev_high_date )
        VALUES
        ( ptn.table_owner, ptn.table_name, ptn.partition_name, DECODE(ptn.subpartition_count, 0, 'N', 'N/A'), 'N', ptn.dict_high_date, ptn.dict_prev_high_date );

        IF ptn.subpartition_count != 0 THEN
            INSERT INTO tw_archive_subpartitions(table_owner, table_name, partition_name, subpartition_name, partition_archived)
            SELECT s.table_owner, s.table_name, s.partition_name, s.subpartition_name, 'N'
            FROM dba_tab_subpartitions s
            WHERE s.table_owner = ptn.table_owner AND s.table_name = ptn.table_name AND s.partition_name = ptn.partition_name;
        END IF;

        COMMIT;
    END LOOP;

    Logger.logExit(g_package, c_procname, NULL);
    JobLogger.jobEnding(c_jobname, 'Main', 0);
EXCEPTION
    WHEN OTHERS THEN
        Logger.logError(g_package, c_procname, SQLERRM);
        JobLogger.jobEnding(c_jobname, 'Exception ORA'||SQLCODE, 0);
        ROLLBACK;
        RAISE;
END identify_partitions;

PROCEDURE import_partitions
IS
    c_jobname CONSTANT VARCHAR2(30) := 'TWAR_IMPORT_PARTITIONS';
    c_prod_date CONSTANT DATE := dat.prod_date;
    c_procname CONSTANT VARCHAR2(30) := 'IMPORT_PARTITIONS';
    v_subpartition_key VARCHAR2(200);
    v_job_id NUMBER;
BEGIN
    IF g_trace THEN
        EXECUTE IMMEDIATE 'BEGIN tw_local_archiver.enable_trace@'||g_tw_link||'; END;';
    END IF;

    v_job_id := JobLogger.jobStarting(c_jobname, c_prod_date);

    Logger.logEntry(g_package, c_procname, NULL);
    -- This will be extended for other tables, at present we handle range partitioned or range-list subpartitioned tables with daily or monthly partitioning scheme
    FOR ptn IN (
        SELECT t.table_owner, t.table_name, p.partition_name, p.high_date, p.prev_high_date, s.high_value, s.subpartition_name
        FROM tw_archive_tables t
        JOIN tw_archive_partitions p ON t.table_owner = p.table_owner AND t.table_name = p.table_name
        LEFT JOIN tw_archive_subpartitions x ON p.table_owner = x.table_owner AND p.table_name = x.table_name AND p.partition_name = x.partition_name
        LEFT JOIN dba_tab_subpartitions s ON p.table_owner = s.table_owner AND p.table_name = s.table_name AND p.partition_name = s.partition_name and s.subpartition_name = x.subpartition_name
        WHERE ( p.partition_archived = 'N' OR p.partition_archived = 'N/A' AND x.partition_archived = 'N' )
        AND ( ( t.partitioning_type = 'DAILY' AND p.high_date <= c_prod_date - t.partitions_online )
            OR (t.partitioning_type = 'MONTHLY' AND p.high_date <= ADD_MONTHS(c_prod_date, 0 - t.partitions_online))
            )
        --AND t.table_owner||'.'||t.table_name not in ('TGL.GAI_BAL_TAB')
        ORDER BY t.table_owner, t.table_name, p.high_date
    ) 
LOOP
        -- Convert the list partition key into a VARCHAR2
        IF ptn.high_value IS NOT NULL THEN
            EXECUTE IMMEDIATE 'BEGIN :x := '||ptn.high_value||'; END;' USING OUT v_subpartition_key;
        END IF;
        IF ptn.table_owner = 'TGL' AND ptn.table_name = 'GAI_BAL_TAB' THEN
            -- We only load GAI_BAL_TAB subpartitions for the CAL and WSS subpartitions, this is the only table that will get special treatment
            --TL 20240817 added INF and RN
            IF v_subpartition_key IN ('CAL', 'WSS', 'INF', 'RN') THEN
                load_range_partition(ptn.table_owner, ptn.table_name, ptn.prev_high_date,  ptn.high_date, ptn.subpartition_name, v_subpartition_key);
                UPDATE tw_archive_subpartitions SET partition_archived = 'Y'
                WHERE table_owner = ptn.table_owner AND table_name = ptn.table_name
                AND partition_name = ptn.partition_name AND subpartition_name = ptn.subpartition_name;
                COMMIT;
            END IF;
        -- In the general case archiving is data driven
        ELSE
            -- Slightly different parameters for partitioned vs. subpartitioned loads
            IF ptn.subpartition_name IS NULL THEN
                load_range_partition(ptn.table_owner, ptn.table_name, ptn.prev_high_date,  ptn.high_date, ptn.partition_name, NULL);
                UPDATE tw_archive_partitions SET partition_archived = 'Y'
                WHERE table_owner = ptn.table_owner AND table_name = ptn.table_name AND partition_name = ptn.partition_name;
                COMMIT;
            ELSE
                load_range_partition(ptn.table_owner, ptn.table_name, ptn.prev_high_date,  ptn.high_date, ptn.subpartition_name, v_subpartition_key);
                UPDATE tw_archive_subpartitions SET partition_archived = 'Y'
                WHERE table_owner = ptn.table_owner AND table_name = ptn.table_name
                AND partition_name = ptn.partition_name AND subpartition_name = ptn.subpartition_name;
                COMMIT;
            END IF;
        END IF;
    END LOOP;
    Logger.logExit(g_package, c_procname, NULL);

    JobLogger.jobEnding(c_jobname, 'Main', 0);
EXCEPTION
    WHEN OTHERS THEN
        Logger.logError(g_package, c_procname, SQLERRM);
        JobLogger.jobEnding(c_jobname, 'Exception ORA'||SQLCODE, 0);
        ROLLBACK;
        RAISE;
END import_partitions;

PROCEDURE purge_partitions
IS
    c_jobname CONSTANT VARCHAR2(30) := 'TWAR_PURGE_PARTITIONS';
    c_prod_date CONSTANT DATE := dat.prod_date;
    c_procname CONSTANT VARCHAR2(30) := 'PURGE_PARTITIONS';
    c_quarter_start CONSTANT DATE := TRUNC(dat.prod_date, 'Q');
    v_job_id NUMBER;
    v_data_found BOOLEAN;
    v_rc sys_refcursor;
    v_stmt VARCHAR2(4000);
    v_dummy PLS_INTEGER;
BEGIN
    v_job_id := JobLogger.jobStarting(c_jobname, c_prod_date);
    Logger.logEntry(g_package, c_procname, NULL);

    -- Purge any tables which are configured for it with daily partitioning
    -- Iterate partitions based on metadata in reverse order from the end of the last month we are archiving
    FOR p IN (
        SELECT p.table_name, p.table_owner, p.partition_name, p.high_date, at.send_to_tape
        , CASE WHEN TRUNC(p.high_date, 'MM') != TRUNC(p.prev_high_date, 'MM') THEN 'Y' ELSE 'N' END last_day
        , COUNT(*) OVER (PARTITION BY at.table_owner, at.table_name ORDER BY p.high_date DESC ) pos
        FROM tw_archive_partitions p
        JOIN tw_archive_tables at ON at.table_owner = p.table_owner AND at.table_name = p.table_name
        WHERE at.purge_interval = 'QUARTERLY'
        AND p.high_date <= ADD_MONTHS(c_quarter_start, at.retain_intervals * -3)
        AND p.partition_purged = 'N'
        AND at.partitioning_type = 'DAILY'
    ) LOOP
        -- All partitions will get exported whether empty or not.  If this export is successful, the partition is safely in the DBFS
        -- which has all the normal Oracle data security stuff and can immediately be dropped.
        -- If we hit an exception exporting (out of space most likely) then this job will terminate.
        -- Space will be cleared by the Tivoli archive job and this one will be retried until it succeeds.
        IF p.send_to_tape = 'Y' THEN
            export_partition(p.table_owner, p.table_name, p.partition_name);
        END IF;

        IF p.last_day = 'Y' THEN
            -- Start working on a new month
            span_info(g_package, c_procname, 'Processing month ending before '||TO_CHAR(p.high_date, 'DD-MON-YYYY'));
            v_data_found := FALSE;
        END IF;

        -- Detect starting archive mid-month, typically because last week's job ran out of space
        IF p.last_day = 'N' AND p.pos = 1 THEN
            span_info(g_package, c_procname, 'Starting archiving from mid-month at '||TO_CHAR(p.high_date, 'DD-MON-YYYY'));
            v_data_found := TRUE;
        END IF;

        IF NOT v_data_found THEN
            -- Probe current partition and see if it contains data
            v_stmt := 'SELECT 1 FROM "'||p.table_owner||'"."'||p.table_name||'" PARTITION ("'||p.partition_name||'") WHERE ROWNUM = 1';
            OPEN v_rc FOR v_stmt;
            FETCH v_rc INTO v_dummy;
            IF v_rc%FOUND THEN
                -- Keep this day as it is month end, subsequent days this month will be removed
                v_data_found := TRUE;
                span_info(g_package, c_procname, 'Keeping partition '||p.partition_name||' for month end data');
                UPDATE tw_archive_partitions SET partition_purged = 'KEPT'
                WHERE table_owner = p.table_owner AND table_name = p.table_name AND partition_name = p.partition_name;
                COMMIT;
            ELSE
                CLOSE v_rc;
                -- Non-business day at end of month, remove partition
                UPDATE tw_archive_partitions SET partition_purged = 'Y'
                WHERE table_owner = p.table_owner AND table_name = p.table_name AND partition_name = p.partition_name;
                COMMIT;
                v_stmt := 'ALTER TABLE "'||p.table_owner||'"."'||p.table_name||'" DROP PARTITION "'||p.partition_name||'"';
                span_info(g_package, c_procname, v_stmt);
            END IF;
        ELSE
            -- Mid-month data, we don't need it
            UPDATE tw_archive_partitions SET partition_purged = 'Y'
            WHERE table_owner = p.table_owner AND table_name = p.table_name AND partition_name = p.partition_name;
            COMMIT;
            v_stmt := 'ALTER TABLE "'||p.table_owner||'"."'||p.table_name||'" DROP PARTITION "'||p.partition_name||'"';
            span_info(g_package, c_procname, v_stmt);
        END IF;
    END LOOP;

    purge_monthly_partitions;

    Logger.logExit(g_package, c_procname, NULL);
    JobLogger.jobEnding(c_jobname, 'Main', 0);
EXCEPTION
    WHEN OTHERS THEN
        Logger.logError(g_package, c_procname, SQLERRM);
        JobLogger.jobEnding(c_jobname, 'Exception ORA'||SQLCODE, 0);
        ROLLBACK;
        RAISE;
END purge_partitions;

-- Private procedures
PROCEDURE purge_monthly_partitions
IS
    c_prod_date CONSTANT DATE := dat.prod_date;
    c_procname CONSTANT VARCHAR2(30) := 'PURGE_MONTHLY_PARTITIONS';
    c_jobname CONSTANT VARCHAR2(30) := 'TWAR_PURGE_MONTHLY_PARTITIONS';
    c_quarter_start CONSTANT DATE := TRUNC(dat.prod_date, 'Q');
    v_rc sys_refcursor;
    v_stmt VARCHAR2(4000);
    v_last_day DATE;
    v_ret NUMBER;
    v_tmp_table VARCHAR2(30);

    CURSOR c_candidate_index(p_table_owner IN VARCHAR2, p_table_name IN VARCHAR2) IS
        SELECT k.column_name, t.subpartitioning_type, t.def_tablespace_name, c.index_name
        FROM dba_part_key_columns k
        JOIN dba_part_tables t ON k.name = t.table_name AND k.owner = t.owner
        LEFT JOIN (
            SELECT c.table_owner, c.table_name, c.column_name, c.column_position, i.index_name
            FROM dba_ind_columns c
            JOIN dba_indexes i ON c.index_owner = i.owner AND c.index_name = i.index_name AND i.index_type = 'NORMAL'
        ) c ON k.owner = c.table_owner AND k.name = c.table_name AND k.column_name = c.column_name AND c.column_position = 1
        WHERE k.owner = p_table_owner AND k.name = p_table_name AND k.object_type = 'TABLE';

    r_candidate_index c_candidate_index%ROWTYPE;
    v_job_id NUMBER;
BEGIN
    Logger.logEntry(g_package, c_procname, NULL);
    v_job_id := JobLogger.jobStarting(c_jobname, c_prod_date);

    -- Iterate partitions which need purging
    FOR p IN (
        SELECT p.table_name, p.table_owner, p.partition_name, p.high_date, t.send_to_tape, t.retain_month_end_data
        FROM tw_archive_partitions p
        JOIN tw_archive_tables t ON t.table_owner = p.table_owner AND t.table_name = p.table_name
        WHERE t.partitioning_type = 'MONTHLY'
        AND p.partition_purged = 'N'
        AND p.high_date <= ADD_MONTHS(TRUNC(dat.prod_date, 'Q'), t.retain_intervals * -3)
        AND t.purge_interval = 'QUARTERLY'
        ORDER BY p.high_date
    ) LOOP
        IF p.send_to_tape = 'Y' THEN
            NULL;
            --export_partition(p.table_owner, p.table_name, p.partition_name);
        END IF;

        IF p.retain_month_end_data = 'Y' THEN
            -- Establish last business day in the month
            -- First, is there a local B-tree index with the partition key on the leading edge (we know this key is a date)
            OPEN c_candidate_index(p.table_owner, p.table_name);
            FETCH c_candidate_index INTO r_candidate_index;
            IF r_candidate_index.index_name IS NOT NULL THEN
                CLOSE c_candidate_index;
                -- Use a stopkey scan on this index because that is a fast way to do it
                v_stmt := 'SELECT * FROM ( SELECT /*+ INDEX_DESC(t '||r_candidate_index.index_name||') */ "'
                    ||r_candidate_index.column_name||'" FROM "'||p.table_owner||'"."'||p.table_name||'" PARTITION( "'||p.partition_name||'") t '
                    ||'WHERE "'||r_candidate_index.column_name||'" IS NOT NULL ORDER BY "'||r_candidate_index.column_name||'" DESC '
                    ||') WHERE ROWNUM = 1';
            ELSE
                CLOSE c_candidate_index;
                -- Full scan in parallel as we don't have an obvious index to use
                v_stmt := 'SELECT /*+ PARALLEL(t 12) FULL(t) */ MAX("'||r_candidate_index.column_name
                    ||'") FROM "'||p.table_owner||'"."'||p.table_name||'" PARTITION( "'||p.partition_name||'") t';
            END IF;

            span_info(g_package, c_procname, 'Running following query to establish last business day in '||p.partition_name);
            span_info(g_package, c_procname, v_stmt);
            OPEN v_rc FOR v_stmt;
            FETCH v_rc INTO v_last_day;
            CLOSE v_rc;
            span_info(g_package, c_procname, 'Keeping data for '||v_last_day);

            IF r_candidate_index.subpartitioning_type = 'LIST' THEN
                -- Iterate subpartitions
                FOR s IN (
                    SELECT s.subpartition_name, s.high_value, k.column_name
                    FROM dba_tab_subpartitions s
                    JOIN dba_subpart_key_columns k ON k.owner = p.table_owner AND k.name = p.table_name AND k.object_type = 'TABLE'
                    WHERE s.table_owner = p.table_owner AND s.table_name = p.table_name AND s.partition_name = p.partition_name
                ) LOOP
                    shrink_monthly_partition( p.table_owner, p.table_name, s.subpartition_name, r_candidate_index.def_tablespace_name
                        , r_candidate_index.column_name, v_last_day, r_candidate_index.subpartitioning_type, s.column_name, s.high_value);
                END LOOP;
            ELSIF r_candidate_index.subpartitioning_type = 'NONE' THEN
                shrink_monthly_partition( p.table_owner, p.table_name, p.partition_name, r_candidate_index.def_tablespace_name
                    , r_candidate_index.column_name, v_last_day, r_candidate_index.subpartitioning_type, NULL, NULL);
            ELSE
                RAISE_APPLICATION_ERROR(-20001, 'Unsupported partitioning scheme detected for '||p.table_owner||'.'||p.table_name);
            END IF;
        END IF;
    END LOOP;

    JobLogger.jobEnding(c_jobname, 'Main', 0);
    Logger.logExit(g_package, c_procname, NULL);
EXCEPTION
    WHEN OTHERS THEN
        Logger.logError(g_package, c_procname, SQLERRM);
        JobLogger.jobEnding(c_jobname, 'Exception ORA'||SQLCODE, 0);
        ROLLBACK;
        RAISE;
END purge_monthly_partitions;

PROCEDURE shrink_monthly_partition
( p_table_owner IN VARCHAR2
, p_table_name IN VARCHAR2
, p_partition_name IN VARCHAR2
, p_tablespace_name IN VARCHAR2
, p_partition_key IN VARCHAR2
, p_partition_value IN DATE
, p_subpartitioning_type IN VARCHAR2
, p_subpartition_key IN VARCHAR2
, p_subpartition_value IN VARCHAR2
) IS
    c_procname CONSTANT VARCHAR2(30) := 'SHRINK_MONTHLY_PARTITION';
    v_stmt VARCHAR2(4000);
    v_ret NUMBER;
    v_tmp_table VARCHAR2(30);
BEGIN
    Logger.logEntry(g_package, c_procname, 'Shrink partition '||p_table_owner||'.'||p_table_name||' ('||p_partition_name||')');
    -- Create temp table for one day only, load it from a query and exchange it for the fully populated version
    SELECT 'TMP_'||tmp_table_sq01.NEXTVAL INTO v_tmp_table FROM dual;

    v_stmt := 'CREATE TABLE '||p_table_owner||'.'||v_tmp_table||' ROW STORE COMPRESS ADVANCED PCTFREE 0 TABLESPACE '||p_tablespace_name
        ||' AS SELECT * FROM '||p_table_owner||'.'||p_table_name||' WHERE 1=0';

    v_ret := execute_stmt(v_stmt);

    v_stmt := 'INSERT /*+ APPEND PARALLEL(4) */ INTO '||p_table_owner||'.'||v_tmp_table
        ||' SELECT /*+ PARALLEL(4) */ * FROM '||p_table_owner||'.'||p_table_name
        ||' WHERE '||p_partition_key||' = TO_DATE('''||TO_CHAR(p_partition_value, 'DD-MON-YYYY')||''', ''DD-MON-YYYY'')'
        ||CASE WHEN p_subpartitioning_type = 'LIST' THEN 'AND '||p_subpartition_key||' = '||p_subpartition_value END;

    v_ret := execute_stmt(v_stmt);

    exchange_partition(p_table_owner, p_table_name, p_partition_name, v_tmp_table, p_subpartitioning_type);
    rebuild_indexes(p_table_owner, p_table_name, p_partition_name, p_subpartitioning_type);

    Logger.logExit(g_package, c_procname, NULL);
END shrink_monthly_partition;

PROCEDURE load_range_partition
( p_owner IN VARCHAR2
, p_table_name IN VARCHAR2
, p_low_date IN DATE
, p_high_date IN DATE
, p_partition_name IN VARCHAR2
, p_subpartition_key IN VARCHAR2
) IS
    c_procname CONSTANT VARCHAR2(30) := 'LOAD_RANGE_PARTITION';
    v_stmt VARCHAR2(4000);
    v_tmp_table VARCHAR2(30);
    v_part_key dba_part_key_columns.column_name%TYPE;
    v_subpartitioning_type dba_part_tables.subpartitioning_type%TYPE;
    v_tablespace_name dba_part_tables.def_tablespace_name%TYPE;
    rc NUMBER;
BEGIN
    Logger.logEntry(g_package, c_procname, TO_CHAR(p_low_date, 'DD-MON-YYYY')||', '||TO_CHAR(p_high_date, 'DD-MON-YYYY')||','||p_partition_name);
    dbms_application_info.set_module('Load '||p_table_name, TO_CHAR(p_low_date, 'DD-MON-YYYY')||' '||p_subpartition_key);

    -- Retrieve parameters from the dictionary
    SELECT subpartitioning_type, def_tablespace_name
    INTO v_subpartitioning_type, v_tablespace_name
    FROM dba_part_tables
    WHERE owner = p_owner AND table_name = p_table_name;

    SELECT column_name INTO v_part_key
    FROM dba_part_key_columns WHERE owner = p_owner AND name = p_table_name AND object_type = 'TABLE';

    -- Validate options are sensible and supported
    IF v_subpartitioning_type NOT IN ('NONE', 'LIST') THEN
        RAISE_APPLICATION_ERROR(-20000, 'Unsupported subpartitioning scheme '||v_subpartitioning_type||' for '||p_owner||'.'||p_table_name);
    ELSIF v_subpartitioning_type = 'NONE' AND p_subpartition_key IS NOT NULL THEN
        RAISE_APPLICATION_ERROR(-20000, 'Subpartition key provided for non-subpartitioned table '||p_owner||'.'||p_table_name);
    ELSIF v_subpartitioning_type = 'LIST' AND p_subpartition_key IS NULL THEN
        RAISE_APPLICATION_ERROR(-20000, 'No subpartition key provided for subpartitioned table '||p_owner||'.'||p_table_name);
    END IF;

    SELECT 'TMP_'||tmp_table_sq01.NEXTVAL INTO v_tmp_table FROM dual;

    v_stmt := 'CREATE TABLE '||p_owner||'.'||v_tmp_table||' ROW STORE COMPRESS ADVANCED PCTFREE 0 TABLESPACE '||v_tablespace_name
        ||' AS SELECT * FROM '||p_owner||'.'||p_table_name||' WHERE 1=0';

    rc := execute_stmt(v_stmt);

    -- Modify storage for regular LOB or collection types to achieve compression.
    -- TODO: Pick storage attributes up from the dictionary in the archive DB
    -- This may well not work for other types of LOBs and nested tables are definitely not supported
    FOR l IN (
        SELECT l.column_name, c.data_type, c.data_type_owner, t.typecode
        FROM dba_lobs l
        JOIN dba_tab_columns c ON c.owner = l.owner AND c.table_name = l.table_name AND c.column_name = l.column_name
        LEFT JOIN dba_types t ON c.data_type_owner = t.owner AND c.data_type = t.type_name
        WHERE l.owner = p_owner AND l.table_name = p_table_name
    ) LOOP
        IF l.typecode IS NULL THEN
            -- Regular LOB
            v_stmt := 'ALTER TABLE '||p_owner||'.'||v_tmp_table||' MOVE LOB('||l.column_name||') STORE AS (COMPRESS HIGH)';
        ELSIF l.typecode = 'COLLECTION' THEN
            -- VARRAY at least based on examples so far tested
            v_stmt := 'ALTER TABLE '||p_owner||'.'||v_tmp_table||' MOVE VARRAY '||l.column_name||' STORE AS SECUREFILE LOB (COMPRESS HIGH)';
        ELSE
            RAISE_APPLICATION_ERROR(-20001, 'Unsupported LOB encountered trying to archive '||p_owner||'.'||p_table_name);
        END IF;

        rc := execute_stmt(v_stmt);
    END LOOP;

    SELECT 'INSERT /*+ APPEND PARALLEL(4) */ INTO '||p_owner||'.'||v_tmp_table||' ( '||cols||' ) '
        ||'SELECT /*+ FULL(t) PARALLEL(4) */ '||cols||' FROM '||p_owner||'.'||p_table_name||'@'||g_tw_link||' t '
        ||'WHERE '||v_part_key||' >= TO_DATE('''||TO_CHAR(p_low_date, 'DD-MON-YYYY')||''', ''DD-MON-YYYY'') '
        ||'AND '||v_part_key||' < TO_DATE('''||TO_CHAR(p_high_date, 'DD-MON-YYYY')||''', ''DD-MON-YYYY'') '
        ||CASE WHEN v_subpartitioning_type = 'LIST' THEN 'AND '||subpartition_key||'= '''||p_subpartition_key||'''' END
    INTO v_stmt
    FROM (
        SELECT LISTAGG(c.column_name, ', ') WITHIN GROUP (ORDER BY c.column_id) cols
        , k.column_name subpartition_key
        FROM dba_tab_columns c
        LEFT JOIN dba_subpart_key_columns k ON k.owner = c.owner AND k.name = c.table_name AND k.object_type = 'TABLE'
        WHERE c.owner = p_owner and c.table_name = p_table_name
        GROUP BY k.column_name
    );

    rc := execute_stmt(v_stmt);
    span_info(g_package, c_procname, rc||' rows created');

    span_info(g_package, c_procname, 'Gathering stats on '||v_tmp_table);
    dbms_application_info.set_module('Stats '||p_table_name, TO_CHAR(p_low_date, 'DD-MON-YYYY')||' '||p_subpartition_key);
    dbms_stats.gather_table_stats(p_owner, v_tmp_table, NULL, dbms_stats.auto_sample_size);

    exchange_partition(p_owner, p_table_name, p_partition_name, v_tmp_table, v_subpartitioning_type);

    rebuild_indexes(p_owner, p_table_name, p_partition_name, v_subpartitioning_type);

    Logger.logExit(g_package, c_procname, NULL);
END load_range_partition;

FUNCTION execute_stmt
( p_stmt IN VARCHAR2
) RETURN PLS_INTEGER IS
    c_procname CONSTANT VARCHAR2(30) := 'EXECUTE_STMT';
    rval PLS_INTEGER;
BEGIN
    span_info(g_package, c_procname, p_stmt);
    EXECUTE IMMEDIATE p_stmt;

    rval := SQL%ROWCOUNT;
    RETURN rval;
END execute_stmt;

PROCEDURE rebuild_indexes
( p_owner IN VARCHAR2
, p_table_name IN VARCHAR2
, p_partition_name IN VARCHAR2
, p_subpartitioning_type IN VARCHAR2
) IS
    c_procname CONSTANT VARCHAR2(30) := 'REBUILD_INDEXES';
    v_stmt VARCHAR2(4000);
    rc NUMBER;
BEGIN
    Logger.logEntry(g_package, c_procname, 'Index rebuild '||p_table_name||'.'||p_table_name||'('||p_partition_name||')');

    -- Loop indexes and rebuild relevant (sub)partition
    -- This implicitly constrains the data model to have only local indexes and may break if this constraint is not met
    -- LOB indexes are excluded, they do not need rebuilding
    FOR ind IN (
        SELECT i.index_name
        FROM dba_part_indexes p
        JOIN dba_indexes i ON i.owner = p.owner AND i.index_name = p.index_name
        WHERE p.table_name = p_table_name AND p.owner = p_owner
        AND p.locality = 'LOCAL'
        AND i.index_type != 'LOB'
    ) LOOP
        dbms_application_info.set_module(c_procname, 'Index rebuild '||p_table_name||'.'||p_table_name||'('||p_partition_name||')');
        v_stmt := 'ALTER INDEX '||p_owner||'.'||ind.index_name||' REBUILD '||CASE WHEN p_subpartitioning_type = 'LIST' THEN 'SUB' END||'PARTITION '||p_partition_name||' PARALLEL 8';
        rc := execute_stmt(v_stmt);
    END LOOP;
    Logger.logExit(g_package, c_procname, NULL);
END rebuild_indexes;

PROCEDURE exchange_partition
( p_owner IN VARCHAR2
, p_table_name IN VARCHAR2
, p_partition_name IN VARCHAR2
, p_tmp_table_name IN VARCHAR2
, p_subpartitioning_type IN VARCHAR2
) IS
    c_procname CONSTANT VARCHAR2(30) := 'EXCHANGE_PARTITION';
    v_stmt VARCHAR2(4000);
    rc NUMBER;
    e_locked EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_locked, -54);
    v_exchange_done BOOLEAN;
BEGIN
    Logger.logEntry(g_package, c_procname, 'Exchange partition '||p_table_name||'.'||p_table_name||'('||p_partition_name||') with '||p_tmp_table_name);
    span_info(g_package, c_procname, 'Exchanging '||p_tmp_table_name||' with '||p_partition_name);
    v_exchange_done := FALSE;
    LOOP
        EXIT WHEN v_exchange_done;
        BEGIN
            v_stmt := 'ALTER TABLE '||p_owner||'.'||p_table_name||' EXCHANGE '||CASE WHEN p_subpartitioning_type = 'LIST' THEN 'SUB' END||'PARTITION '||p_partition_name||
                ' WITH TABLE '||p_owner||'.'||p_tmp_table_name||' WITHOUT VALIDATION';
            rc := execute_stmt(v_stmt);
            v_exchange_done := TRUE;
        EXCEPTION
            WHEN e_locked THEN
                dbms_application_info.set_action(p_table_name||'.'||p_table_name||'('||p_partition_name||') waiting');
                dbms_lock.sleep(ROUND(dbms_random.value(10,60)));
        END;
    END LOOP;

    v_stmt := 'DROP TABLE '||p_owner||'.'||p_tmp_table_name||' PURGE';
    --rc := execute_stmt(v_stmt);
    Logger.logExit(g_package, c_procname, NULL);
END exchange_partition;

PROCEDURE export_partition
( p_owner IN VARCHAR2
, p_table_name IN VARCHAR2
, p_partition_name IN VARCHAR2
) AS
    c_procname CONSTANT VARCHAR2(30) := 'EXPORT_PARTITION';
    c_status_mask CONSTANT NUMBER := dbms_datapump.ku$_status_job_error + dbms_datapump.ku$_status_job_status + dbms_datapump.ku$_status_wip;
    v_params archive_datapump_params%ROWTYPE;
    jobh NUMBER;
    v_job_state VARCHAR2(30);  -- To keep track of job state
    v_sts ku$_Status;        -- The status object returned by get_status
    e_exporterr EXCEPTION;
    e_job_does_not_exist EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_job_does_not_exist,-31626);
BEGIN
    Logger.logEntry(g_package, c_procname, p_partition_name);
    dbms_application_info.set_module(g_package, 'Exporting '||p_partition_name);

    -- Load config
    SELECT * INTO v_params FROM archive_datapump_params;

    -- Loop single iteration as validation
    FOR x IN (
        SELECT table_owner, table_name, partition_name
        FROM dba_tab_partitions
        WHERE table_owner = p_owner AND table_name = p_table_name AND partition_name = p_partition_name
    ) LOOP
        span_info(g_package, c_procname, 'Creating datapump job for '||p_partition_name);
        jobh := dbms_datapump.open (operation => 'EXPORT', job_mode => 'TABLE', job_name => x.partition_name, version => 'LATEST');

        dbms_datapump.add_file
        ( handle    => jobh
        , filename  => x.table_name||'_'||x.partition_name||'%U.dmp'
        , reusefile => 1
        , directory => v_params.dmpfile_directory
        , filetype  => dbms_datapump.ku$_file_type_dump_file
        , filesize  => ''||v_params.file_size||''
        );

        dbms_datapump.add_file
        ( handle    => jobh
        , filename  => x.table_name||'_'||x.partition_name||'.log'
        , directory => v_params.logfile_directory
        , filetype  => dbms_datapump.ku$_file_type_log_file
        );

        dbms_datapump.set_parallel (handle => jobh, degree => v_params.degree);

        IF v_params.compression='Y' THEN
            dbms_datapump.set_parameter(handle => jobh, name => 'COMPRESSION', value => 'ALL');
        END IF;

        -- CONTENT = INCLUDE_METADATA
        dbms_datapump.set_parameter (handle => jobh, name => 'INCLUDE_METADATA', value  => 0);

        -- Set filters for the required partition
        dbms_datapump.metadata_filter (handle => jobh, name => 'SCHEMA_EXPR', value  => 'IN ('''||x.table_owner||''')');
        dbms_datapump.metadata_filter (handle => jobh, name => 'NAME_EXPR', value  => 'IN ('''||x.table_name||''')');

        dbms_datapump.data_filter
        ( handle => jobh
        , name => 'PARTITION_LIST'
        , value => x.partition_name
        , table_name => x.table_name
        , schema_name => x.table_owner
        );

        span_info(g_package, c_procname, 'Starting datapump job '||jobh);
        -- Start job asynchronously
        dbms_datapump.start_job (handle => jobh);

        v_job_state := 'UNDEFINED';
        WHILE (v_job_state != 'COMPLETED') AND (v_job_state != 'STOPPED') LOOP
            dbms_lock.sleep(30);
            logger.logDebug(g_package, c_procname, 'Job_state='||v_job_state);
            -- Read status with a bit mask
            dbms_datapump.get_status
            ( handle => jobh
            , mask => c_status_mask
            , timeout => -1
            , job_state => v_job_state
            , status => v_sts
            );
            IF (BITAND(v_sts.mask, dbms_datapump.ku$_status_job_error) != 0) THEN
                -- Stop the export job and raise an error
                dbms_datapump.stop_job(handle => jobh);
                dbms_datapump.detach(handle => jobh);
                RAISE e_exporterr;
            END IF;
        END LOOP;
        span_info(g_package, c_procname, 'Detected completion of datapump job');
        dbms_datapump.detach(handle => jobh);
    END LOOP;

    Logger.logExit(g_package, c_procname, NULL);
EXCEPTION
    WHEN e_exporterr THEN
        RAISE;
    WHEN e_job_does_not_exist THEN
        span_info(g_package, c_procname, 'Exception: Datapump job does not exist anymore. It has completed.');
END export_partition;

PROCEDURE span_info
( p_package IN VARCHAR2
, p_procname IN VARCHAR2
, p_message IN VARCHAR2) IS
    v_offset PLS_INTEGER := 1;
    c_msglen PLS_INTEGER := LENGTH(p_message);
    c_maxlen PLS_INTEGER := 2000;
BEGIN
    -- Chop messages up into 2000 character pieces before sending them to the logger because it cannot accomodate larger messages
    -- If overlong messages are sent, the logger will handle the resulting exception and simply ignore any further messages of the same type.
    -- Really this logic belongs in the central logging package IMO
    LOOP
        Logger.logInfo(p_package, p_procname, SUBSTR(p_message, v_offset, c_maxlen));
        v_offset := v_offset + c_maxlen;
        EXIT WHEN v_offset > c_msglen;
    END LOOP;
END span_info;

END tw_archiver;
/
