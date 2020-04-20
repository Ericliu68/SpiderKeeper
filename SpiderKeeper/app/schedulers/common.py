import threading
import time
import datetime


from sqlalchemy import and_


from SpiderKeeper.app import scheduler, app, agent, db
from SpiderKeeper.app.spider.model import Project, JobInstance, SpiderInstance, JobExecution


def sync_job_execution_status_job():
    '''
    sync job execution running status
    :return:
    '''
    for project in Project.query.all():
        agent.sync_job_status(project)
    app.logger.debug('[sync_job_execution_status]')


def sync_spiders():
    '''
    sync spiders
    :return:
    '''
    for project in Project.query.all():
        spider_instance_list = agent.get_spider_list(project)
        SpiderInstance.update_spider_instances(project.id, spider_instance_list)
    app.logger.debug('[sync_spiders]')


def run_spider_job(job_instance_id):
    '''
    run spider by scheduler
    :param job_instance:
    :return:
    '''
    try:
        job_instance = JobInstance.find_job_instance_by_id(job_instance_id)
        agent.start_spider(job_instance)
        app.logger.info('[run_spider_job][project:%s][spider_name:%s][job_instance_id:%s]' % (
            job_instance.project_id, job_instance.spider_name, job_instance.id))
    except Exception as e:
        app.logger.error('[run_spider_job] ' + str(e))


def reload_runnable_spider_job_execution():
    '''
    add periodic job to scheduler
    :return:
    '''
    running_job_ids = set([job.id for job in scheduler.get_jobs()])
    # app.logger.debug('[running_job_ids] %s' % ','.join(running_job_ids))
    available_job_ids = set()
    # add new job to schedule
    for job_instance in JobInstance.query.filter_by(enabled=0, run_type="periodic").all():
        job_id = "spider_job_%s:%s" % (job_instance.id, int(time.mktime(job_instance.date_modified.timetuple())))
        available_job_ids.add(job_id)
        if job_id not in running_job_ids:
            try:
                scheduler.add_job(run_spider_job,
                                  args=(job_instance.id,),
                                  trigger='cron',
                                  id=job_id,
                                  minute=job_instance.cron_minutes,
                                  hour=job_instance.cron_hour,
                                  day=job_instance.cron_day_of_month,
                                  day_of_week=job_instance.cron_day_of_week,
                                  month=job_instance.cron_month,
                                  second=0,
                                  max_instances=999,
                                  misfire_grace_time=60 * 60,
                                  coalesce=True)
            except Exception as e:
                app.logger.error(
                    '[load_spider_job] failed {} {},may be cron expression format error '.format(job_id, str(e)))
            app.logger.info('[load_spider_job][project:%s][spider_name:%s][job_instance_id:%s][job_id:%s]' % (
                job_instance.project_id, job_instance.spider_name, job_instance.id, job_id))
    # remove invalid jobs
    for invalid_job_id in filter(lambda job_id: job_id.startswith("spider_job_"),
                                 running_job_ids.difference(available_job_ids)):
        scheduler.remove_job(invalid_job_id)
        app.logger.info('[drop_spider_job][job_id:%s]' % invalid_job_id)


# 定时删除数据库过期数据
def delete_database():
    try:
        start_time = datetime.datetime.now() - datetime.timedelta(days=app.config["DEL_DATABASE"])
        db.session.query(JobExecution).filter(JobExecution.create_time < start_time).delete()
        db.session.commit()
        app.logger.info("历史记录删除成功")
    except Exception as e:
        app.logger.info('历史删除数据库内容')
        app.logger.error(e)
        db.session.rollback()
        db.session.remove()


# 定时删除等待或者运行超过一小时的任务
def delete_run_status():
    try:

        start_time = datetime.datetime.now() - datetime.timedelta(minutes=app.config["JOBS_TIMEOVER"])
        # 等待任务超过一个小时的
        db.session.query(JobExecution).filter(and_(JobExecution.create_time < start_time, JobExecution.running_status == 0)).delete()

        db.session.commit()
        # 运行任务超过一个小时的
        # db.session.query(JobExecution).filter(and_(JobExecution.create_time < start_time, JobExecution.running_status == 1))\
        #     .update({JobExecution.running_status: 2})
        #
        # db.session.commit()
        running_jobs = db.session.query(JobExecution).filter(and_(JobExecution.create_time < start_time, JobExecution.running_status == 1)).all()
        for running_job in running_jobs:
            agent.cancel_spider(running_job)

        app.logger.info("超时任务删除成功")
    except Exception as e:
        app.logger.info('超时任务数据库内容')
        app.logger.error(e)
        db.session.rollback()
        db.session.remove()


