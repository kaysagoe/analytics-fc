"""Tester target sink class, which handles writing streams."""


from singer_sdk.sinks import BatchSink


class TesterSink(BatchSink):
    """Tester target sink class."""

    max_size = 10000  # Max records to write in one batch

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        # Sample:
        # ------
        # client.upload(context["file_path"])  # Upload file
        # Path(context["file_path"]).unlink()  # Delete local copy

        print(context['records'])
