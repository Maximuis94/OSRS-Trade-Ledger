"""
Module with GameUpdate class implementation


"""


from datetime import datetime
from typing import Any, Dict, Optional


class GameUpdate:
    """
    Representation of a game update.

    This class encapsulates all essential details of a game update, including metadata,
    textual content, publication information, and optional additional data.

    Parameters
    ----------
    title : str
        The title or headline of the game update.
    publish_date : datetime
        The date (and optionally time) when the update was published.
    content : str
        The full textual description or body content of the update.
    metadata : Optional[Dict[str, Any]], optional
        A dictionary containing extra information about the update such as version,
        tags, author, or any other metadata (by default None).

    Attributes
    ----------
    title : str
        The title or headline of the game update.
    publish_date : datetime
        The date when the update was published.
    content : str
        The full text content of the update.
    metadata : Dict[str, Any]
        A dictionary of additional metadata related to the update.

    Examples
    --------
    >>> from datetime import datetime
    >>> update = GameUpdate(
    ...     title="Sailing Alpha Live Now!",
    ...     publish_date=datetime.strptime("2025-03-27", "%Y-%m-%d"),
    ...     content="The update includes new sailing features and significant gameplay improvements.",
    ...     metadata={"version": "1.0", "author": "OSRS Team"}
    ... )
    >>> print(update)
    GameUpdate(title=Sailing Alpha Live Now!, publish_date=2025-03-27 00:00:00)
    """
    
    def __init__(self,
                 title: str,
                 publish_date: datetime,
                 content: str,
                 metadata: Optional[Dict[str, Any]] = None) -> None:
        self.title: str = title
        self.publish_date: datetime = publish_date
        self.content: str = content
        self.metadata: Dict[str, Any] = metadata if metadata is not None else {}
    
    def __repr__(self) -> str:
        """
        Return the official string representation of the GameUpdate.

        Returns
        -------
        str
            A string representation of the GameUpdate instance.
        """
        return (
            f"GameUpdate(title={self.title}, "
            f"publish_date={self.publish_date.isoformat()})"
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the GameUpdate instance to a dictionary.

        This is useful for scenarios such as serialization or logging.

        Returns
        -------
        Dict[str, Any]
            A dictionary representation of the GameUpdate.
        """
        return {
            "title": self.title,
            "publish_date": self.publish_date.isoformat(),
            "content": self.content,
            "metadata": self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GameUpdate":
        """
        Create a GameUpdate instance from a dictionary.

        Parameters
        ----------
        data : Dict[str, Any]
            A dictionary containing keys 'title', 'publish_date', 'content',
            and optionally 'metadata' corresponding to the GameUpdate attributes.

        Returns
        -------
        GameUpdate
            A new instance of GameUpdate initialized with the provided data.

        Raises
        ------
        ValueError
            If any required field is missing from the input dictionary.
        """
        try:
            title = data["title"]
            publish_date_str = data["publish_date"]
            content = data["content"]
            metadata = data.get("metadata", {})
            publish_date = datetime.fromisoformat(publish_date_str)
        except KeyError as e:
            raise ValueError(f"Missing required field: {e}")
        return cls(title=title, publish_date=publish_date, content=content, metadata=metadata)