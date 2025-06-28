fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/vitess/vtgate.proto")?;
    tonic_build::compile_protos("proto/vitess/vtgateservice.proto")?;
    tonic_build::compile_protos("proto/vitess/vschema.proto")?;
    tonic_build::compile_protos("proto/vitess/vtctldata.proto")?;
    tonic_build::compile_protos("proto/vitess/vtctlservice.proto")?;
    tonic_build::compile_protos("proto/vitess/tabletmanagerdata.proto")?;
    tonic_build::compile_protos("proto/vitess/logutil.proto")?;
    tonic_build::compile_protos("proto/vitess/replicationdata.proto")?;
    tonic_build::compile_protos("proto/vitess/mysqlctl.proto")?;
    tonic_build::compile_protos("proto/vitess/vtadmin.proto")?;

    Ok(())
}
