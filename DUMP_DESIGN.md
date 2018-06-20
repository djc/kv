# Request dumper design

For a debugging tool like this, the most important design consideration would
seem to be that it should have a minimal performance impact on the system;
both to make it possible to use even in high-performance situation and to
prevent that the tool itself changes the behavior of the system too much.

There are three major factors in performance:

- (Re)serializing a request

  This could hopefully be prevented by intercepting requests at an early
  stage, before deserialization has happened. However, the overall design
  of the system or performance considerations might make this harder.

- Write latency for persistent media

  In order to make sure the request data has successfully been persisted,
  the request data will need to be fully flushed to persistent media
  (typically a disk -- hopefully an SSD or even NVME storage). While this
  is happening, the backing memory cannot be reused or freed, incurring
  a memory usage penalty for the application.

  It is likely attractive to let the I/O be handled by a separate thread.
  The resulting memory lifetime challenges could be addressed by maintaining
  a reference count for the backing memory (assuming that the request memory
  does not need to be mutated), so that the memory will not be dropped before
  both the request processing and the persistence have finished.

- Memory allocation concerns

  If the request data is mutated, it might be more straightforward to copy
  the request memory, which would also forego the need for reference counting.
  However, depending on the size of the request data, this might be expensive
  in both CPU time (for copying the memory) and the extra memory allocation.

As an optimization, in some scenarios (for example, if the captured data is
to be used for profiling), it might be possible to do take random samples
instead of capturing all the data (for example by dumping each request with
only a 10% chance).

In addition to raw request data, the tool should probably also capture limited
metadata, in particular the time that the request was received, with sufficient
resolution. In case of very high volumes, the process of getting a timestamp
may have to be optimized in order not to swamp the system with extra syscalls.
A simple length-prefixed serialization format can be used to save the metadata
with the request data.
