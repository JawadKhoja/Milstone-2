from google.cloud import pubsub_v1  # pip install google-cloud-pubsub
import glob
import base64
import os

def set_google_credentials():
    """Search for a service account JSON file and set the environment variable."""
    service_account_files = glob.glob("*.json")
    if not service_account_files:
        raise FileNotFoundError("No service account JSON file found in the current directory.")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_files[0]

def initialize_publisher(project_id, topic_name):
    """Initialize a Pub/Sub publisher with ordering enabled."""
    publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
    publisher = pubsub_v1.PublisherClient(publisher_options=publisher_options)
    topic_path = publisher.topic_path(project_id, topic_name)
    return publisher, topic_path

def get_image_files(image_folder, extension="*.png"):
    """Retrieve a list of image files from the specified folder."""
    image_files = glob.glob(os.path.join(image_folder, extension))
    if not image_files:
        raise FileNotFoundError("No image files found in the specified folder.")
    return image_files

def publish_images(publisher, topic_path, image_files):
    """Publish images to Pub/Sub with ordering keys."""
    for image_path in image_files:
        try:
            with open(image_path, "rb") as f:
                encoded_image = base64.b64encode(f.read()).decode("utf-8")
            key = os.path.basename(image_path)  # Use file name as ordering key
            future = publisher.publish(topic_path, encoded_image.encode("utf-8"), ordering_key=key)
            future.result()  # Ensure message is published
            print(f"Successfully published {key}.")
        except Exception as e:
            print(f"Failed to publish {key}: {e}")

def main():
    set_google_credentials()
    project_id = "velvety-study-448822-h6"
    topic_name = "Image2Redis"
    image_folder = "Dataset_Occluded_Pedestrian/"
    
    publisher, topic_path = initialize_publisher(project_id, topic_name)
    print(f"Publishing messages with ordering keys to {topic_path}.")
    
    try:
        image_files = get_image_files(image_folder)
        publish_images(publisher, topic_path, image_files)
        print("All messages have been published.")
    except FileNotFoundError as e:
        print(e)

if __name__ == "__main__":
    main()
