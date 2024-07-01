import torch


def get_model(model_name: str):
    """
    Returns model object, it must be a callable object.
    :param model_name: type of model to be returned
    :return:
    """

    supported_objects = ['yolov5l', 'yolov5s']

    if model_name not in supported_objects:
        raise ValueError(f"Model name {model_name} not supported. Please use one of {supported_objects}")

    model = None
    if model_name == 'yolov5l':
        model = torch.hub.load('ultralytics/yolov5', 'yolov5l', pretrained=True)
        model.eval()
    elif model_name == 'yolov5s':
        model = torch.hub.load('ultralytics/yolov5', 'yolov5s', pretrained=True)
        model.eval()
    return model