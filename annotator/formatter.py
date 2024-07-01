from abc import ABC, abstractmethod
from typing import Iterable
from PIL.Image import Image
import os
import cv2
import numpy as np

class AnnotationFormatter(ABC):
    @abstractmethod
    def save(self, dest: str, predictions: Iterable, image: Image, *args, **kwargs) -> None:
        pass


class RaceCarYOLOAnnotationFormatter(AnnotationFormatter):

    RACE_CAR_CLASS_INDEX = 2

    def __get_opencv_img_from_pil(self, pil_img):
        np_img = np.array(pil_img)
        return cv2.cvtColor(np_img, cv2.COLOR_RGB2BGR)

    def save(self, dest: str, predictions: Iterable, img: Image, verbose = False) -> None:
        """
        Saves annotations in the YOLO format
        :param dest: where to save the annotations
        :param predictions: predictions already in the correct format
        :param img: the image
        :return:
        """
        width, height = img.size
        # Get the detection results
        detections = predictions.xywh[0]  # xywh format

        # Filter detections for race cars
        race_car_detections = [detection for detection in detections if int(detection[-1]) == self.RACE_CAR_CLASS_INDEX]

        # Prepare the annotation file content
        annotations = []
        detection = None
        for detection in race_car_detections:
            x_center, y_center, bbox_width, bbox_height, confidence, class_idx = detection
            annotations.append(
                f"{int(class_idx)} {x_center / width:.6f} {y_center / height:.6f} {bbox_width / width:.6f} {bbox_height / height:.6f}\n")
        if detection is not None:
            print(f"Detection found for {dest}")
        else:
            print(f"No detection found for {dest}")
        # Save the annotations to a file
        annotation_file_path = os.path.splitext(dest)[0] + ".txt"
        with open(annotation_file_path, 'w') as f:
            f.writelines(annotations)

        print(f"Annotations saved to {annotation_file_path}")

        # Display detection when debugging
        if verbose and detection is not None:
            img_cv2 = self.__get_opencv_img_from_pil(img)
            print(detection)
            for detection in race_car_detections:
                x_center, y_center, bbox_width, bbox_height, confidence, class_idx = detection
                x_center, y_center, bbox_width, bbox_height = x_center.item(), y_center.item(), bbox_width.item(), bbox_height.item()

                # Convert to top-left corner format for OpenCV
                x1 = int((x_center - bbox_width / 2))
                y1 = int((y_center - bbox_height / 2))
                x2 = int((x_center + bbox_width / 2))
                y2 = int((y_center + bbox_height / 2))

                print(x1, y1, x2, y2)
                print(img_cv2.shape)
                # Draw bounding box
                cv2.rectangle(img_cv2, (x1, y1), (x2, y2), (0, 255, 0), 2)

            # Display the image with bounding boxes
            cv2.imshow("Detected Race Cars", img_cv2)
            cv2.waitKey(0)
            cv2.destroyAllWindows()

def get_formatter(formatter_strategy: str) -> AnnotationFormatter:
    """
    Factory method for a formatter object.
    :param formatter_strategy:
    :return:
    """

    legal_strategies = ['yolov5l', 'yolov5s']

    if formatter_strategy not in legal_strategies:
        raise ValueError(f"{formatter_strategy} not a valid formatter strategy, please use one of {legal_strategies}.")

    if formatter_strategy == 'yolov5l' or formatter_strategy == 'yolov5s':
        return RaceCarYOLOAnnotationFormatter()
