from abc import ABC, abstractmethod
import argparse
from PIL import Image
from annotator.annotation_models import get_model
from annotator.formatter import get_formatter

class Annotator(ABC):

    @abstractmethod
    def annotate(self, data_path: str, annotation_strategy: str) -> str:
        """
        Given path to data and a destination, saves an annotation at the same location as the data input
        :param data_path: path to data
        :return: tuple of destination and annotation paths
        """
        pass

class RaceCarAnnotator(Annotator):

    def annotate(self, data_path: str, annotation_strategy: str) -> str:
        """
        Given a path to an image, saves an annotation of the data as a text file in the
        yolov5 format in the same directory as the original image with the name <image_path>.txt
        :param data_path: path to rgb image
        :param annotation_strategy: types of models and annotation format to be used.
        :return:
        """
        img = Image.open(data_path)
        model = get_model(annotation_strategy)
        annotation_formatter = get_formatter(annotation_strategy)
        predictions = model(img)
        dest = data_path.replace('jpeg', 'txt')
        annotation_formatter.save(dest, predictions, img)


def get_annotator(annotation_strategy: str) -> Annotator:

    legal_annotation_strategies = ["racecar"]
    if annotation_strategy not in legal_annotation_strategies:
        raise ValueError(f"annotation strategy {annotation_strategy} not valid, please use one of {legal_annotation_strategies}")

    if annotation_strategy == "racecar":
        return RaceCarAnnotator()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=str, required=True, help="Path to the image file")
    parser.add_argument("--destination", type=str, required=True, help="Path to save the annotation file")
    parser.add_argument("--weights", type=str, default="yolov5s.pt", help="Path to the model weights file")
    parser.add_argument("--verbose", action='store_true', help="Display bounding boxes on found objects")
    args = parser.parse_args()

    # Load the YOLOv5 model from PyTorch Hub
    annotator = RaceCarAnnotator()
    annotator.annotate(args.source, args.destination, 'yolov5l')