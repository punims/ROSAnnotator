from abc import ABC, abstractmethod
from annotator.annotator import get_annotator
from dataset_parser.datset_parser import get_parser
import os
class AnnotationPipeline(ABC):

    def __init__(self, parser_type: str, annotator_type: str):
        self.dataset_parser = get_parser(parser_type)
        self.annotator = get_annotator(annotator_type)
    @abstractmethod
    def annotate(self, source: str, dest: str) -> None:
        """
        All annotation pipelines must implement this method
        An annotation pipeline takes in a parser and annotator and uses them to annotate an entire dataset.
        :param source: Path to the source dataset
        :param dest: Where to save the dataset and their respective annotations
        :return:
        """
        pass


class RaceCarAnnotationPipeline(AnnotationPipeline):

    def __init__(self, parser_type: str = 'racecar', annotator_type: str = 'racecar'):
        super().__init__(parser_type, annotator_type)

    def annotate(self, source: str, dest: str) -> None:
        """
        Annotates a racecar dataset. The dataset is saved as a db3 file and placed at source
        All images will be saved with their respective annotations inside dest.
        All annotations will have the same name as the source image, placed in the same directory as their respective
        images.
        :param source: source of the db3 dataset
        :param dest: base location of the extracted images and their respective annotations
        :return:
        """

        self.dataset_parser.parse(source, dest)

        jpeg_paths = self.__get_jpeg_paths(dest)
        for jpeg_path in jpeg_paths:
            self.annotator.annotate(jpeg_path, annotation_strategy='yolov5s')


    def __get_jpeg_paths(self, source) -> list[str]:
        """
        returns list of paths of all jpeg images in source recursively
        :param source:
        :return:
        """

        jpeg_files = []
        for root, _, files in os.walk(source):
            for file in files:
                if file.lower().endswith(('.jpg', '.jpeg')):
                    absolute_path = os.path.abspath(os.path.join(root, file))
                    jpeg_files.append(absolute_path)
        return jpeg_files





