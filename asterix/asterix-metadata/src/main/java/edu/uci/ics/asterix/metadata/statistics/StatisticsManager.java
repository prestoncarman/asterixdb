/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.metadata.statistics;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataRecordTypes;
import edu.uci.ics.asterix.metadata.entitytupletranslators.BaseStatisticsTupleTranslator;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * @author rico
 * 
 */
public class StatisticsManager {

    private static final StatisticsManager INSTANCE = new StatisticsManager();
    private static final Logger LOGGER = Logger.getLogger(StatisticsManager.class.getName());
    private String metadatastore;

    private StatisticsManager() {
        // Singelton
    }

    public static StatisticsManager getInstance() {
        return INSTANCE;
    }

    public File getStatisticsFile(String dataverseName, String datasetName) {
        return new File(this.metadatastore + dataverseName + File.separator + datasetName + "_stats");
    }

    public Long getTupleCount(String dataverseName, String datasetName, String nodeId) {
        List<BaseStatistics> stats;
        try {
            stats = getStatisticsObjects(dataverseName, datasetName);
        } catch (AsterixException e) {
            LOGGER.info(e.getLocalizedMessage());
            return null;
        }
        if (stats == null)
            return null;
        for (BaseStatistics stat : stats) {
            if (nodeId.equals(stat.getNodeId())) {
                return (stat).getTupleCount();
            }
        }
        return null;
    }

    private List<BaseStatistics> getStatisticsObjects(String dataverseName, String datasetName) throws AsterixException {
        File f = getStatisticsFile(dataverseName, datasetName);
        if (!f.exists()) {
            return null;
        }
        FileInputStream oin;
        try {
            oin = new FileInputStream(f);
        } catch (FileNotFoundException e) {
            LOGGER.info(e.getLocalizedMessage());
            return null;
        }
        BaseStatisticsTupleTranslator statsTranslator = new BaseStatisticsTupleTranslator(true,
                MetadataRecordTypes.BASE_STATISTICS_RECORDTYPE.getFieldNames().length);
        List<BaseStatistics> stats;
        try {
            stats = statsTranslator.parseInputstream(oin);
        } catch (IOException e) {
            LOGGER.info(e.getLocalizedMessage());
            return null;
        } finally {
            try {
                oin.close();
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "", e);
            }
        }
        return stats;
    }

    public void saveStatistics(BaseStatistics stats) {
        final String dataverseName = stats.getAqlSourceId().getDataverseName();
        final String datasetName = stats.getAqlSourceId().getDatasetName();
        List<BaseStatistics> savedStats = null;
        try {
            savedStats = getStatisticsObjects(dataverseName, datasetName);
        } catch (AsterixException e) {
            LOGGER.log(Level.SEVERE, "", e);
        }
        if (savedStats != null) {
            BaseStatistics toRemove = null;
            for (BaseStatistics stat : savedStats) {
                if (stats.getNodeId().equals(stat.getNodeId())) {
                    toRemove = stat;
                    break;
                }
            }
            if (toRemove != null) {
                savedStats.remove(toRemove);
            }
        } else {
            savedStats = new ArrayList<BaseStatistics>(1);
        }
        savedStats.add(stats);
        writeStatisticsObjects(savedStats, dataverseName, datasetName);
    }

    private void writeStatisticsObjects(List<BaseStatistics> savedStats, String dataverseName, String datasetName) {
        File f = getStatisticsFile(dataverseName, datasetName);
        if (!f.exists()) {
            try {
                f.createNewFile();
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "", e);
            }
        }
        LOGGER.info(f.getAbsolutePath());
        PrintStream ps = null;
        try {
            ps = new PrintStream(new BufferedOutputStream(new FileOutputStream(f)));
        } catch (FileNotFoundException e) {
            LOGGER.log(Level.SEVERE, "", e);
            return;
        }
        try {
            toADM(savedStats, ps);
            ps.flush();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "", e);
        } catch (MetadataException e) {
            LOGGER.log(Level.SEVERE, "", e);
        } finally {
            ps.close();
        }
    }

    public void toADM(List<BaseStatistics> stats, PrintStream ps) throws MetadataException, IOException {
        BaseStatisticsTupleTranslator statsTranslator = new BaseStatisticsTupleTranslator(true,
                MetadataRecordTypes.BASE_STATISTICS_RECORDTYPE.getFieldNames().length);
        for (BaseStatistics statObj : stats) {
            ITupleReference tupleRef = statsTranslator.getTupleFromMetadataEntity(statObj);
            statsTranslator.printTupleToPS(tupleRef, ps);
        }
    }

    public String getMetadatastore() {
        return metadatastore;
    }

    public void setMetadatastore(String metadatastore) {
        LOGGER.info("Setting metadatastore: " + metadatastore);
        this.metadatastore = metadatastore;
    }
}
