package cromwell.backend.impl.vk

import cromwell.backend.{BackendConfigurationDescriptor, BackendJobDescriptor}
import cromwell.core.path.Path
import skuber.Resource.Quantity
import skuber.Volume.{Mount, PersistentVolumeClaimRef}
import skuber.batch.Job
import skuber.{Container, LocalObjectReference, ObjectMeta, Pod, Resource, RestartPolicy, Volume}
import wdl4s.parser.MemoryUnit

final case class VkTask(jobDescriptor: BackendJobDescriptor,
                        configurationDescriptor: BackendConfigurationDescriptor,
                        vkPaths: VkJobPaths,
                        runtimeAttributes: VkRuntimeAttributes,
                        containerWorkDir: Path,
                        dockerImageUsed: String,
                        jobShell: String) {

  private val workflowDescriptor = jobDescriptor.workflowDescriptor
  private val workflowId = workflowDescriptor.id
  private val rootWorkflowId = workflowDescriptor.rootWorkflowId
  private val fullyQualifiedTaskName = jobDescriptor.taskCall.localName.replaceAll("_", "-").toLowerCase()
  private val index = jobDescriptor.key.index.getOrElse(0)
  private val attempt = jobDescriptor.key.attempt
  val name: String = fullyQualifiedTaskName + "-" + index + "-" + attempt + "-" + workflowId.shortString

  // contains the script to be executed
  val commandScriptPath = vkPaths.callExecutionDockerRoot.resolve("script").toString

  val ram :: _ = Seq(runtimeAttributes.memory) map {
    case Some(x) =>
      Option(x.to(MemoryUnit.GB).amount)
    case None =>
      None
  }
  val cpu = runtimeAttributes.cpu.getOrElse(ram.getOrElse(2.0)/2)
  val memory = ram.getOrElse(cpu * 2)
  val resources = if(runtimeAttributes.gpuType.isEmpty && runtimeAttributes.gpuType.isEmpty){
    Option(Resource.Requirements(
      requests = Map(
        "cpu" -> Quantity(cpu.toString),
        "memory" -> Quantity(memory.toString+"Gi"),
      ),
      limits = Map(
        "cpu" -> Quantity(cpu.toString),
        "memory" -> Quantity(memory.toString+"Gi"),
      )
    ))
  } else {
    val gpu = runtimeAttributes.gpuCount.map(_.value.toString).get
    val gpuType = runtimeAttributes.gpuType.get
    Option(Resource.Requirements(
      requests = Map(
        "cpu" -> Quantity(cpu.toString),
        "memory" -> Quantity(memory.toString+"Gi"),
        gpuType -> Quantity(gpu),
      ),
      limits = Map(
        "cpu" -> Quantity(cpu.toString),
        "memory" -> Quantity(memory.toString+"Gi"),
        gpuType -> Quantity(gpu),
      )
    ))
  }

  val pvc = if (configurationDescriptor.backendConfig.hasPath("pvc")){
    configurationDescriptor.backendConfig.getString("pvc")
  }else{
    ""
  }
  val mountPath = if (configurationDescriptor.backendConfig.hasPath("dockerRoot")){
    configurationDescriptor.backendConfig.getString("dockerRoot")
  }else{
    configurationDescriptor.backendConfig.getString("root")
  }

  var mounts = List[Mount]()
  if(!pvc.equals("")) {
    mounts = mounts :+ Mount(
      name = pvc,
      mountPath = mountPath
    )
  }
  if(!runtimeAttributes.disks.isEmpty){
    for(disk <- runtimeAttributes.disks.get){
      mounts = mounts :+ Mount(
        name = disk.name,
        mountPath = disk.mountPoint.pathAsString
      )
    }
  }

  val containers = List(Container(
    name = fullyQualifiedTaskName,
    image = dockerImageUsed,
    command = List(jobShell, commandScriptPath),
    workingDir = runtimeAttributes.dockerWorkingDir,
    resources = resources,
    volumeMounts = mounts
  ))


  val podSpec = Pod.Spec(
    containers = containers,
    volumes = if(!pvc.equals("")) List(Volume(
      name = pvc,
      source = PersistentVolumeClaimRef(
        claimName = pvc
      )
    )) else Nil,
    restartPolicy = RestartPolicy.OnFailure,
    imagePullSecrets = List(LocalObjectReference(
      name = "imagepull-secret"
    ))
  )

  val wdlExecName = if(workflowDescriptor.rootWorkflow.name.length > 64){
    workflowDescriptor.rootWorkflow.name.substring(0,64);
  } else {
    workflowDescriptor.rootWorkflow.name
  }

  val labels = Map(
    "gcs-wdlexec-id" -> rootWorkflowId.toString,
    "system-tag.cci.io/gcs-wdlexec-id" -> rootWorkflowId.toString,
    "system-tag.cci.io/gcs-wdlexec-name" -> wdlExecName,
    "gcs-wdl-name" -> "cromwell",
    "gcs.task.name" -> name,
    "gcs.source.name" -> fullyQualifiedTaskName
  )

  val podMetadata = ObjectMeta(name=fullyQualifiedTaskName,labels = labels)

  val templateSpec = Pod.Template.Spec(metadata=podMetadata).withPodSpec(podSpec)

  val jobMetadata = ObjectMeta(name=name,labels = labels)

  val job = Job(metadata=jobMetadata).withTemplate(templateSpec)

}
