# Workflow

Providing quality data in an efficient manner is the mission of Statistics Norway. Like any other organisation that processes significant amounts of data, we strive to streamline our processes for efficiency and maintainability.

Workflow integrations for automating and monitoring our process addresses some of that. The [](#automation) chapter describes how this plays out in the time series library.

However, our mandate as a national statistics agency, also comes with extra requirements on top of that. Both national law and international agreements dictates principles and formal standards that we must adhere to, and we are subject national and international reviews and audits on a regular basis.

Some of the raw data we receive contain sensitive information.
This raises the bar: We also have to show what we do *not* do. We must both ensure that sensitive data does not leak, and to the extent possible be able to show that it does not.
While logs and audit trails are essential for transparency, they are not allowed to contain any sensitive information.
Also, our entire infrastructure is designed around [a process model and the concept of stable states](#generalised-process-with-stable-states) of data.

Time series *typically* enter the picture at the later stages of the statistics production process. That is, after sensitive and person identifying information identifiers have been removed and row level editing has been completed.
The time series data is *likely* to be aggregated to a level where it is no longer considered sensitive.

The audit and [process requirements still apply](#generalised-process-with-stable-states), and both operational monitoring and audits calls for tracking of [data lineage](#data-lineage).

## Generalised process with stable states

We apply a generalised process model to all our statistics production pipelines.

A cornerstone of our implementation is the concept of "stable states". In these, we persist *snapshots* of the data in standardised formats and naming conventions.
The snapshots are stored "forever".
That means we can go back at any time and review the data as it was before.

The first stages removes sensitive information, standardises the data and does row level QA editing where applicable.

These requirements and the conventions implemented to meet them are specific to Statistics Norway and applies not only to timeseries data. As such, the standardisation is created to be generic, not to be optimal for dealing with timeseries.

The solution is to keep the working data repository and the persisted snapshots separate. While a `.save` function stores a timeseries dataset in its working repository, `snapshot` will copy it to the archive.

```python
x = Dataset(...)

# save the dataset to a timeseries working repository
x.save()

# persist to stable stage
x.snapshot()
```

The process model is based on the international standard [Generic Statistical Business Process Model (GSBPM)](https://unece.org/statistics/specifications/gsbpm).

While with proper configuration, the `snapshot` function will work, it is not expected to be of much use outside a SSB context.

Data is also transferred to the National Archive. That does not involve the time series library as the implementation relies on the snapshots to stable states.

Documenting processes and tracking [data lineage](#data-lineage) are strict requirements, not nice to haves.

## Automation

Beyond very small scale, automation improves efficiency and consistency.
It can also help enforce the formal process.

It also ensures the same procedure is used every time. This is not the same as no errors occurring. When circumstances change, for example if inputs arrive late or out their normal order, a normally working procedure may fail.
Process automation can take many forms, from simple schedules and event triggers to intricate patterns of events triggering processes based on complex criteria.
Every automation mechanism also introduces its own potential for errors.

Taking away the human in front of the screen when starting processes not only frees up human resources to focus on other tasks,
it uncouples checking that everything ran as it should from running the process.
With automation, the need for improved logging and monitoring rises.

SSB Time series is one tool among others.
Workflow control is assumed to be external to the library,
so the library does not in itself provide any means of automation.

It is, however, designed with *event driven* automation and monitoring systems in mind.
All significant *events* (all read and write operations) are logged in a structured way, using log level INFO.
That enables machine based monitoring and alerting.
Events can also be put on messages queues to be consumed by orchestrating software or trigger processes directly.

This plays into what should be considered a dataset:

Strictly speaking, any collection of data can be considered a dataset.
The dataset class implementation requires all series in the set to be of the same type.
It is easier to name the set if there is some common denominator in the series descriptions.
Many operations make more sense if the dataset is not to sparse, ie if there is a value for all or most dates.
Workflows becomes much tidyer and a lot easier to set up if we add the requirement that all series orginates *from the same technical process at roughly the same time*.


## Data Lineage

Keeping track of where data comes from becomes much easier if all process chains or production pipeline are one directional.
Patterns where multiple processes write to the same set should be avoided. So should recursions.
*That is, to the extent possible.*
This rule is convention only, and not absolute, but it makes life significantly easier when all the series in a set originates from the same process.

Transparency requirements manifest themselves in several features of the time series library:

Several features help keep track of data lineage:

**Structured logging:** The time series library logs all read and write operations with log level `INFO`.
By picking up and attaching *process identifiers* (this is a planned extension) to the log items, logs will provide complete information at the process and dataset level.

**Calculation audit trails:** Most dataset class calculation methods will accumulate information about names of operations performed and datasets involved. While this goes a long way to provide insight into the exact nature of the relationship between datasets. This information will be found in `Dataset.lineage`.

Unfortunately, this approach is not guaranteed to be complete. `Dataset.data` is represented as an Arrow table, and can be served as Pandas or Polars dataframes to any libraries supporting thses formats. That opens a world of possibilities, but at the cost of automatic data lineage. The result of any calculations done outside the library can be written back to same or new datasets as long as it abides to the schema restrictions.

**Built in versioning:** The time series model provides several ways of taking care of versions of the data "as of" a specific time. Or not. Practical choices when using the library amounts to "implicit" data modelling, and the user must be aware of the [information model meaning](./info-model.md#sets-series-and-types) of implemntation details like choice of {py:class}`~ssb_timeseries.types.SeriesType`.


**Persisting data in stable states** as required by the process model, allows going back and inspecting data as previously produced, shared  or published, even if time series versioning was not the pracical choice, or if the "wrong" choice was made.

## Quality indicators all the way down

For time series,

 * missing values (holes in the series) can be observed or counted directly,
 * various statistical methods may be used to measure the variations in the material and identify extreme values,
 * quality indicators may be inherited from non time series data from earlier process steps.

Quality indicators do not themselves provide lineage, but may give an idea of what should be found if tracing back.
The other way around, can good lineage allow accumulating quality indicators after the fact (although it is hard to do this on a detailed level).


* [Stable states](#generalised-process-with-stable-states) of data are possibly the most prominent feature of a *formal process* that underlies infrastrucutre designs that sandboxes our production pipelines.

 * [Automation](#automation) increases efficiency and consistency, but can also help document or even enforce our processes.

* [Data lineage](#data-lineage) and audit trails play an important role in operational monitoring, but are also used for formal audits.
