CREATE DATABASE IF NOT EXISTS data_construct
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE data_construct;

-- Platform metadata only. Generated business/test data belongs in target sinks.
CREATE TABLE IF NOT EXISTS data_template (
  template_id VARCHAR(64) NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  `desc` TEXT NOT NULL,
  target_type VARCHAR(64) NOT NULL,
  target_table VARCHAR(255) NOT NULL,
  batch_size INT UNSIGNED NOT NULL,
  fields_json JSON NOT NULL,
  sink_conf JSON NOT NULL,
  creator VARCHAR(128) NOT NULL DEFAULT '',
  create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT chk_data_template_batch_size CHECK (batch_size > 0),
  INDEX idx_data_template_name (name)
);

CREATE TABLE IF NOT EXISTS data_dag (
  dag_id VARCHAR(64) NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  dag_content JSON NOT NULL,
  cron_expr VARCHAR(128) NULL,
  status ENUM('enabled', 'disabled') NOT NULL DEFAULT 'disabled',
  creator VARCHAR(128) NOT NULL,
  create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_data_dag_status (status)
);

CREATE TABLE IF NOT EXISTS data_job (
  job_id VARCHAR(64) NOT NULL PRIMARY KEY,
  dag_id VARCHAR(64) NOT NULL,
  status ENUM('queued', 'running', 'succeeded', 'failed', 'cancelled') NOT NULL DEFAULT 'queued',
  start_time DATETIME(3) NULL,
  end_time DATETIME(3) NULL,
  trigger_type ENUM('manual', 'schedule', 'api') NOT NULL,
  context_key VARCHAR(255) NOT NULL,
  create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_data_job_dag_created (dag_id, create_time),
  INDEX idx_data_job_status_created (status, create_time),
  CONSTRAINT fk_data_job_dag
    FOREIGN KEY (dag_id) REFERENCES data_dag(dag_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS data_task (
  task_id VARCHAR(64) NOT NULL PRIMARY KEY,
  job_id VARCHAR(64) NOT NULL,
  template_id VARCHAR(64) NOT NULL,
  node_id VARCHAR(64) NOT NULL,
  status ENUM('pending', 'running', 'succeeded', 'failed', 'skipped') NOT NULL DEFAULT 'pending',
  error_msg TEXT NULL,
  batch_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
  start_time DATETIME(3) NULL,
  end_time DATETIME(3) NULL,
  create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_data_task_job_node (job_id, node_id),
  INDEX idx_data_task_template (template_id),
  INDEX idx_data_task_job_status (job_id, status),
  CONSTRAINT fk_data_task_job
    FOREIGN KEY (job_id) REFERENCES data_job(job_id) ON DELETE CASCADE,
  CONSTRAINT fk_data_task_template
    FOREIGN KEY (template_id) REFERENCES data_template(template_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS data_job_log (
  log_id VARCHAR(64) NOT NULL PRIMARY KEY,
  job_id VARCHAR(64) NOT NULL,
  task_id VARCHAR(64) NULL,
  level ENUM('debug', 'info', 'warn', 'error') NOT NULL DEFAULT 'info',
  message TEXT NOT NULL,
  create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_data_job_log_job_time (job_id, create_time),
  INDEX idx_data_job_log_task_time (task_id, create_time),
  CONSTRAINT fk_data_job_log_job
    FOREIGN KEY (job_id) REFERENCES data_job(job_id) ON DELETE CASCADE,
  CONSTRAINT fk_data_job_log_task
    FOREIGN KEY (task_id) REFERENCES data_task(task_id) ON DELETE SET NULL
);
