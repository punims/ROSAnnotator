from abc import ABC, abstractmethod
import sqlite3
import cv2
import numpy as np
from multiprocessing import Process
import os


class DatasetParser(ABC):
    """
    Abstract class for all dataset parsers
    """

    @abstractmethod
    def parse(self, source: str, dest: str) -> None:
        """
        Parses the raw datset in source into a format usable by a corresponding annotator
        :param source: path to raw dataset
        :param dest: destination for the parsed dataset
        """
        pass


def vimba_as_numpy_ndarray(
        byteArray, height=772, width=1032, bits_per_channel=8, bitsPerPixel=10) -> np.ndarray:
    """
    Converts a byte array into a numpy array
    :param byteArray:
    :param height:
    :param width:
    :param bits_per_channel:
    :param bitsPerPixel:
    :return:
    """
    channels_per_pixel = bitsPerPixel // bits_per_channel
    return np.ndarray(
        shape=(height, width, channels_per_pixel),
        buffer=byteArray,
        dtype=np.uint8 if bits_per_channel == 8 else np.uint16,
    )


def get_opencv_img_from_buffer(buffer, flags, CONVERT_BGR: bool) -> np.ndarray:
    """
    Converts a byte array into an opencv image
    :param buffer:
    :param flags:
    :param CONVERT_BGR:
    :return:
    """
    bytes_as_np_array = np.frombuffer(buffer, dtype=np.uint8)
    img = cv2.imdecode(bytes_as_np_array, flags)
    if CONVERT_BGR:
        return cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
    else:
        return img


def readBlobData(files, destination, frame, skipCount=1, CONVERT_BGR: bool = True) -> None:
    """
    Reads db3 database per frame object, converts frames into images (skipping frames according to skipcount)
    and saves them as a jpeg
    :param files: db3 file path
    :param destination: base directory where to save the images
    :param frame: frame topic to extract from the db3 file
    :param skipCount: how many frames to skip ahead in case of subsampling
    :param CONVERT_BGR: convert image from BGR to RGB
    :return:
    """
    try:
        topicQuery = f"%/camera/{frame}/image/compressed"
        os.makedirs(os.path.join(destination, frame), exist_ok=True)

        sqliteConnection = None
        num_frame = 0
        topicId = -1
        verbose = False

        print("Writing " + frame)
        for dbName in files:
            sqliteConnection = sqlite3.connect(dbName)
            cursor = sqliteConnection.cursor()
            if verbose:
                print("Connected to SQLite")

            if topicQuery is not None:
                cursor.execute("SELECT id, name FROM topics WHERE name LIKE ?", (topicQuery,))
                row = cursor.fetchone()
                if row is None:
                    print(f"Topic {topicQuery} not found in {dbName}")
                    continue
                topicId = int(row[0])
                if verbose:
                    print("topicName: ", topicQuery, ", topicId: ", topicId)

            jpegMode = "compressed" in topicQuery

            cursor.execute("SELECT COUNT(*) FROM messages WHERE topic_id = ?", (topicId,))
            total_messages = cursor.fetchone()[0]

            print("Total frames in the database: ", total_messages)
            for offset in range(0, total_messages, skipCount):
                cursor.execute(
                    """SELECT * FROM messages WHERE topic_id = ? LIMIT 1 OFFSET ?""",
                    (topicId, offset)
                )

                done = False
                while not done:
                    row = cursor.fetchone()
                    if row is None:
                        done = True
                        break
                    num_frame = num_frame + 1
                    topic_id_from_message = row[1]

                    if topic_id_from_message == topicId:
                        image_name = frame + f"_{num_frame}.jpeg"
                        final_name = os.path.join(destination, frame, image_name)

                        if not os.path.exists(final_name):
                            timestamp = row[2]
                            photo = row[3]

                            if jpegMode:
                                index = photo.find(b"JFIF")
                                jpgArray = photo[index - 6:]  # offset where image data starts
                            else:
                                index = photo.find(b"bayer")
                                if index == -1 or index > 0x40:
                                    index = photo.find(b"bgr") + 3
                                if index == -1 or index > 0x40:
                                    index = 0
                                jpgArray = photo[index:]  # offset where image data starts

                            if jpegMode:
                                img = get_opencv_img_from_buffer(jpgArray, cv2.IMREAD_ANYCOLOR, CONVERT_BGR)
                            else:
                                # raw image format
                                frame = vimba_as_numpy_ndarray(
                                    jpgArray,
                                    height=1544,
                                    width=2064,
                                    bits_per_channel=8,
                                    bitsPerPixel=24,
                                )
                                # Use opencv to convert raw Bayer image to RGB image
                                img = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)

                            cv2.imwrite(final_name, img)

        print("Finished writing " + frame)
        cursor.close()
    except sqlite3.Error as error:
        print("Failed to read blob data from sqlite table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            if verbose:
                print("sqlite connection is closed")


class RacecarROSBagParser(DatasetParser):

    def __init__(self, CONVERT_BGR=True, SKIP_FRAMES=1):
        super().__init__()
        self.__frames = [
            "front_left",
            "front_right",
            "front_left_center",
            "front_right_center",
            "rear_left",
            "rear_right"
        ]
        self.__CONVERT_BGR = CONVERT_BGR
        self.__SKIP_FRAMES = SKIP_FRAMES

    def parse(self, source: str, dest: str) -> None:
        """
        Parses a race-car datasert ROSbag from source and saves the image dataset at dest
        :param source: source of .db3 file
        :param dest: root for image directory extracted from the .db3 file.
        :return:
        """

        file_path = source
        processes = []
        for frame in self.__frames:
            p = Process(target=readBlobData, args=([file_path], dest, frame, self.__SKIP_FRAMES, self.__CONVERT_BGR))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()


def get_parser(parser_type: str) -> DatasetParser:
    """
    getter for a parser object
    :param parser_type: type of parser
    :return:
    """

    legal_parsers = ["racecar"]
    if parser_type not in legal_parsers:
        raise ValueError(
            f"{parser_type} is not a legal type of parser, please use one of the following {legal_parsers}")

    if parser_type == "racecar":
        return RacecarROSBagParser()
