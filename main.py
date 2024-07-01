from annotation_pipelines import RaceCarAnnotationPipeline
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=str, help="Source for db3 file")
    parser.add_argument("--destination", type=str, help="Destination for extracted images and annotations")
    args = parser.parse_args()

    pipeline = RaceCarAnnotationPipeline()
    pipeline.annotate(args.source, args.destination)