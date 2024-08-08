class BatchStatus:
    def __init__(self, workflow_id, workflow_name, status, start_time, end_time, duration, failed_step_name=None, err_msg=None):
        self.workflow_id = workflow_id
        self.workflow_name = workflow_name
        self.status = status
        self.start_time = start_time
        self.end_time = end_time
        self.duration = duration
        self.failed_step_name = failed_step_name
        self.err_msg = err_msg

    def to_dict(self):
        return {
            "workflowId" : self.workflow_id,
            "workflowName": self.workflow_name,
            "status": self.status,
            "startTime": self.start_time.isoformat() if self.start_time else None,
            "endTime": self.end_time.isoformat() if self.end_time else None,
            "duration": self.duration,
            "failedStepName": self.failed_step_name,
            "errMsg": self.err_msg
        }
