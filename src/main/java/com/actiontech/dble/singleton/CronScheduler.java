package com.actiontech.dble.singleton;

import com.actiontech.dble.backend.datasource.check.AbstractConsistencyChecker;
import com.actiontech.dble.backend.datasource.check.CheckSumChecker;
import com.actiontech.dble.backend.datasource.check.GlobalCheckJob;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.config.model.sharding.table.GlobalTableConfig;
import org.quartz.JobDetail;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static com.actiontech.dble.backend.datasource.check.GlobalCheckJob.GLOBAL_TABLE_CHECK_COUNT;
import static com.actiontech.dble.backend.datasource.check.GlobalCheckJob.GLOBAL_TABLE_CHECK_DEFAULT;
import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

/**
 * Created by szf on 2019/12/19.
 */
public final class CronScheduler {
    private static final CronScheduler INSTANCE = new CronScheduler();
    private volatile org.quartz.Scheduler scheduler;

    private CronScheduler() {

    }

    public void init(Map<String, SchemaConfig> schemaConfigs) throws IOException {
        try {
            if (scheduler != null) {
                scheduler.shutdown();
            }
            Properties props = new Properties();
            props.setProperty("org.quartz.scheduler.skipUpdateCheck", "true");
            props.setProperty("org.quartz.jobStore.class", "org.quartz.simpl.RAMJobStore");
            props.setProperty("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
            props.setProperty("org.quartz.threadPool.threadCount", "4");
            SchedulerFactory schdFact = new StdSchedulerFactory(props);
            scheduler = schdFact.getScheduler();
            // and start it off
            scheduler.start();

            for (Map.Entry<String, SchemaConfig> se : schemaConfigs.entrySet()) {
                for (Map.Entry<String, BaseTableConfig> te : se.getValue().getTables().entrySet()) {
                    BaseTableConfig config = te.getValue();
                    if (config instanceof GlobalTableConfig) {
                        GlobalTableConfig tableConfig = (GlobalTableConfig) config;
                        if (tableConfig.isGlobalCheck()) {
                            globalTableConsistencyCheck(tableConfig);
                            add(se.getKey(), tableConfig);
                        }
                    }
                }
            }


        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void add(String schema, GlobalTableConfig tableConfig) throws Exception {
        String name = schema + "-" + tableConfig.getName();

        JobDetail job = newJob(GlobalCheckJob.class).
                withIdentity(name, name).
                build();

        job.getJobDataMap().put("tableConfig", tableConfig);
        job.getJobDataMap().put("schema", schema);

        Trigger trigger = newTrigger().
                withIdentity("trigger-for-" + schema + "-" + tableConfig.getName(), name).
                withSchedule(cronSchedule(tableConfig.getCron())).
                forJob(name, name).
                build();

        scheduler.scheduleJob(job, trigger);
    }

    private void globalTableConsistencyCheck(GlobalTableConfig tc) throws Exception {
        String clazz = tc.getCheckClass();
        final Class<?> clz;

        switch (clazz) {
            case GLOBAL_TABLE_CHECK_DEFAULT:
                //skip
            case GLOBAL_TABLE_CHECK_COUNT:
                break;
            default:
                clz = Class.forName(clazz);
                if (!AbstractConsistencyChecker.class.isAssignableFrom(clz)) {
                    throw new IllegalArgumentException("rule function must implements " +
                            CheckSumChecker.class.getName() + ", name=" + clazz);
                }
                //test if the Instance can be create
                AbstractConsistencyChecker test = (AbstractConsistencyChecker) clz.newInstance();
        }
    }


    public static CronScheduler getInstance() {
        return INSTANCE;
    }

}
