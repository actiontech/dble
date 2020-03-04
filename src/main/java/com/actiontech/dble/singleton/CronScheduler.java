package com.actiontech.dble.singleton;

import com.actiontech.dble.backend.datasource.check.AbstractConsistencyChecker;
import com.actiontech.dble.backend.datasource.check.GlobalCheckJob;
import com.actiontech.dble.backend.datasource.check.CheckSumChecker;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import org.quartz.*;
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
                for (Map.Entry<String, TableConfig> te : se.getValue().getTables().entrySet()) {
                    TableConfig config = te.getValue();
                    if (config.isGlobalTable() && config.isGlobalCheck()) {
                        globalTableConsistencyCheck(config);
                        add(se.getKey(), te.getValue(), config.getCron());
                    }
                }
            }


        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void add(String schema, TableConfig tableConfig, String cron) throws Exception {
        String name = new StringBuilder().append(schema).append("-").append(tableConfig.getName()).toString();

        JobDetail job = newJob(GlobalCheckJob.class).
                withIdentity(name, name).
                build();

        job.getJobDataMap().put("tableConfig", tableConfig);
        job.getJobDataMap().put("schema", schema);

        Trigger trigger = newTrigger().
                withIdentity("trigger-for-" + schema + "-" + tableConfig.getName(), name).
                withSchedule(cronSchedule(cron)).
                forJob(name, name).
                build();

        scheduler.scheduleJob(job, trigger);
    }

    private void globalTableConsistencyCheck(TableConfig tc) throws Exception {
        String clazz = tc.getGlobalCheckClass();
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
