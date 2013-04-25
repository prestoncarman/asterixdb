package edu.uci.ics.asterix.aql.translator;

import edu.uci.ics.hyracks.api.job.JobSpecification;

public class StatementExecutionContext {

    private final JobSpecification[] jobSpec;
    private final IPostStatementSuccess postSuccess;
    private final IPostStatementFailure postFailure;

    public StatementExecutionContext(JobSpecification[] jobSpec, IPostStatementSuccess postSuccess,
            IPostStatementFailure postFailure) {
        this.jobSpec = jobSpec;
        this.postSuccess = postSuccess;
        this.postFailure = postFailure;
    }

    public JobSpecification[] getJobSpec() {
        return jobSpec;
    }

    public IPostStatementSuccess getPostSuccess() {
        return postSuccess;
    }

    public IPostStatementFailure getPostFailure() {
        return postFailure;
    }
}
