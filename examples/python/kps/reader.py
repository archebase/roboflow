# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""
Reader for robotics data files (MCAP/BAG).

Provides interface for reading messages from robotics data formats.
"""

from pathlib import Path
from typing import Dict, List, Any, Optional, Iterator
from dataclasses import dataclass
import time


@dataclass
class ChannelInfo:
    """Information about a channel/topic."""
    topic: str
    message_type: str
    message_count: int
    qos: Optional[str] = None


@dataclass
class Message:
    """A single message from a robotics data file."""
    data: Dict[str, Any]
    topic: str
    timestamp_ns: int
    sequence: int
    message_type: str


class MessageIterator:
    """Iterator over messages in a robotics data file."""

    def __init__(
        self,
        reader: "McapReader",
        topics: Optional[List[str]] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None
    ):
        self._reader = reader
        self._topics = topics
        self._start_time = start_time
        self._end_time = end_time
        self._position = 0

    def __iter__(self) -> "MessageIterator":
        return self

    def __next__(self) -> Message:
        while self._position < len(self._reader._messages):
            msg = self._reader._messages[self._position]
            self._position += 1

            # Filter by topic
            if self._topics and msg.topic not in self._topics:
                continue

            # Filter by time
            if self._start_time and msg.timestamp_ns < self._start_time:
                continue
            if self._end_time and msg.timestamp_ns > self._end_time:
                continue

            return msg

        raise StopIteration


class McapReader:
    """
    Reader for MCAP and ROS bag files.

    Uses roboflow to read robotics data files.
    """

    def __init__(self, path: Path):
        """
        Initialize reader for a file.

        Args:
            path: Path to MCAP or BAG file
        """
        self._path = Path(path)
        self._messages: List[Message] = []
        self._channels: Dict[str, ChannelInfo] = {}
        self._start_time_ns: Optional[int] = None
        self._end_time_ns: Optional[int] = None
        self._loaded = False

    def open(self) -> None:
        """Open and read the file."""
        if self._loaded:
            return

        # Import roboflow here to avoid import errors if not installed
        try:
            import robocodec
        except ImportError:
            raise ImportError(
                "robocodec is required. Install with: pip install robocodec"
            )

        reader = robocodec.Reader(str(self._path))

        # Get channel information
        channels = reader.channels()
        for ch in channels:
            self._channels[ch["topic"]] = ChannelInfo(
                topic=ch["topic"],
                message_type=ch.get("schema_name", "unknown"),
                message_count=ch.get("message_count", 0)
            )

        # Read all messages
        sequence = 0
        for msg_dict, channel_info in reader.iter_messages():
            msg = Message(
                data=msg_dict,
                topic=channel_info["topic"],
                timestamp_ns=msg_dict.get("timestamp", int(time.time_ns())),
                sequence=sequence,
                message_type=channel_info.get("schema_name", "unknown")
            )
            self._messages.append(msg)
            sequence += 1

            # Track time range
            ts = msg.timestamp_ns
            if self._start_time_ns is None or ts < self._start_time_ns:
                self._start_time_ns = ts
            if self._end_time_ns is None or ts > self._end_time_ns:
                self._end_time_ns = ts

        self._loaded = True

    @property
    def channels(self) -> Dict[str, ChannelInfo]:
        """Get all channels/topics in the file."""
        if not self._loaded:
            self.open()
        return self._channels

    @property
    def message_count(self) -> int:
        """Get total number of messages."""
        if not self._loaded:
            self.open()
        return len(self._messages)

    @property
    def start_time_ns(self) -> Optional[int]:
        """Get start time in nanoseconds."""
        if not self._loaded:
            self.open()
        return self._start_time_ns

    @property
    def end_time_ns(self) -> Optional[int]:
        """Get end time in nanoseconds."""
        if not self._loaded:
            self.open()
        return self._end_time_ns

    @property
    def duration_ns(self) -> Optional[int]:
        """Get duration in nanoseconds."""
        if self._start_time_ns is None or self._end_time_ns is None:
            return None
        return self._end_time_ns - self._start_time_ns

    def iter_messages(
        self,
        topics: Optional[List[str]] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None
    ) -> Iterator[Message]:
        """
        Iterate over messages.

        Args:
            topics: Optional list of topics to filter by
            start_time: Optional start time in nanoseconds
            end_time: Optional end time in nanoseconds

        Returns:
            Iterator over Message objects
        """
        if not self._loaded:
            self.open()

        return MessageIterator(self, topics, start_time, end_time)

    def get_messages_for_topic(self, topic: str) -> List[Message]:
        """Get all messages for a specific topic."""
        return [m for m in self.iter_messages(topics=[topic])]

    def get_unique_topics(self) -> List[str]:
        """Get list of unique topic names."""
        return list(self._channels.keys())


def read_file(path: Path) -> McapReader:
    """
    Convenience function to read a robotics data file.

    Args:
        path: Path to MCAP or BAG file

    Returns:
        McapReader with loaded data
    """
    reader = McapReader(path)
    reader.open()
    return reader
