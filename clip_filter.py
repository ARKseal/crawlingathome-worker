import os
from multiprocessing import cpu_count

import clip
import datasets
import torch
from PIL import Image
import time
import pickle

cuda = torch.cuda.is_available()
if not cuda:
    torch.set_num_threads(cpu_count())

device = torch.device("cuda") if cuda else torch.device("cpu")
datasets.set_caching_enabled(False)

vmem = torch.cuda.get_device_properties(0).total_memory if cuda else 0
batch_size = 128 * int(vmem / 1800000000) if cuda else cpu_count()
print(f"[crawling@home] batch size = {batch_size}")


class CLIP:
    def __init__(self):
        self.model, self.preprocess = clip.load("ViT-B/32", device=device)
        self.cosine_similarity = torch.nn.CosineSimilarity(dim=1, eps=1e-6)
        self.categories = self.model.encode_text(clip.tokenize(
            ["neutral", "selfie", "illustration, drawing", "toys, play, kids, children", "teddy bear, puppet",
             "animal, bird, mammal, insect" "fashion, clothes", "logo, commercial, ad, advertisement",
             "drawing, painting", "anime, cartoon", "comedy, fun", "romance, love story",
             "thriller, suspense, crime story", "action, action movie", "horror, monster movie", "documentary",
             "news, journalism", "entertainment", "talk show",
             "porn, sex, sperm, nipples, breats, tits, boops, penis, dick, cock, clitoris, vagina, fuck, lust, horny, sexual, lick, licking",
             "porn, sex, sperm, nipples", "porn, sex, sperm, penis, dick, cock", "nipples, breats, tits, boops, sexy",
             "penis, dick, cock", "clitoris, vagina", "sex, fuck, lust, horny, sexual, lick, licking",
             "porn, sex, sexy", "sexy, hot", "sperm, skin", "lust, horny, sexual", "lick, licking, body",
             "anime, hentai, sexy", "cartoon, sexy, sex", "hentai", "anime, sexy, breasts", "hentai"]).to(device))
        self.underaged_categories = self.model.encode_text(clip.tokenize(
            ["teenager, teen", "kid, child, teenager, teen, baby or toddler, underaged, little girl, little boy",
             "kid, child, little girl, little boy", "baby, toddler",
             "adult, woman, man, grownup, grown person,full-aged of legal age", "full-aged, of legal age, adult",
             "woman, man", "adult, woman, man, grownup, grown person,full-aged of legal age"]).to(device))
        self.animal_categories = self.model.encode_text(clip.tokenize(
            ["lifeless object, thing", "thing, object", "material", "furniture", "wall", "house", "tree", "wood",
             "ground", "industry", "table", "bed", "tool", "dress, clothes", "door", "chair", "rock, stone", "human",
             "man", "woman", "man, woman", "animal", "cat", "dog", "cow", "pig", "goat", "sheep", "elephant", "horse",
             "horse, elephant, pig, dog, cat, sheep, goat, animal", "life", "wildlife"]).to(device))

    def similarity_imgalt(self, batch):
        similarity = []
        images = [
            self.preprocess(Image.open(path)).unsqueeze(0).to(device)
            for path in batch["PATH"]
        ]
        max_texts = [anyascii(text) for text in batch["TEXT"]]
        texts = clip.tokenize(max_texts, truncate_text=True).to(device)

        with torch.no_grad():
            image_features = self.model.encode_image(
                torch.cat(images)
            ).float()
            text_features = self.model.encode_text(texts).float()

        for image_feat, text_feat in zip(image_features, text_features):
            similarity.append(
                float(
                    self.cosine_similarity(
                        torch.reshape(text_feat, (1, 512)),
                        torch.reshape(image_feat, (1, 512)),
                    )
                )
            )

        batch["similarity"] = similarity
        batch["image_features"] = image_features.detach().cpu().numpy()
        return batch

    def preprocess_images(self, df):
        im_dataset = datasets.Dataset.from_pandas(df)
        im_dataset = im_dataset.map(self.similarity_imgalt, batched=True, batch_size=256, keep_in_memory=True,
                                    desc="CLIP inference")
        return im_dataset["image_features"], im_dataset["similarity"]

    def prob(self, image_features, text_features):
        with torch.no_grad():
            image_features = torch.as_tensor(image_features).to(device)
            image_features /= image_features.norm(dim=-1, keepdim=True)
            text_features /= text_features.norm(dim=-1, keepdim=True)

            # cosine similarity as logits
            similarity = (100.0 * image_features.float() @ text_features.T.float()).softmax(dim=-1)
            _, indices = similarity.topk(2)
            return indices


clip_filter = CLIP()


def df_clipfilter(df):
    sim_threshold = 0.3
    underaged_text = ["teen", "kid", "child", "baby"]

    img_embedding, similarities = clip_filter.preprocess_images(df)
    tmp_embed = copy(img_embedding)
    for i, img_embed in enumerate(tmp_embed):
        if similarities[i] < sim_threshold:
            df.drop(i, inplace=True)
            img_embedding.remove(img_embed)
            continue

        # get most similar categories
        nsfw_prob = clip_filter.prob(img_embed, clip.categories)
        df.at[i, "NSFW"] = "UNSURE"
        df.at[i, "similarity"] = similarities[i]
        if nsfw_prob[0] < 19 and nsfw_prob[1] < 19:
            df.at[i, "NSFW"] = "UNLIKELY"
            continue
        elif nsfw_prob[0] >= 19 and nsfw_prob[1] >= 19:
            df.at[i, "NSFW"] = "NSFW"

        underage_prob = clip_filter.prob(img_embed, clip.underaged_categories)
        if (
                underage_prob[0] < 4
                or underage_prob[1] < 4
                or any(x in df.at[i, "TEXT"] for x in underaged_text)
        ):
            df.drop(i, inplace=True)
            img_embedding.remove(img_embed)
            continue

        animal_prob = clip_filter.prob(img_embed, clip.animal_categories)
        if animal_prob[0] > 20:
            df.drop(i, inplace=True)
            img_embedding.remove(img_embed)

    df.reset_index(drop=True, inplace=True)
    return df, img_embedding


def df_tfrecords(df, output_fname):
    import tensorflow as tf
    from tfr_image.utils import bytes_feature, int64_feature

    def image_to_tfexample(sample_id, image_data, image_format, height, width, caption):
        return tf.train.Example(
            features=tf.train.Features(
                feature={
                    "sampleID": bytes_feature(sample_id),
                    "image": bytes_feature(image_data),
                    "format": bytes_feature(image_format),
                    "label": bytes_feature(caption),
                    "height": int64_feature(height),
                    "width": int64_feature(width),
                }
            )
        )

    with tf.io.TFRecordWriter(output_fname) as tfrecord_writer:
        for i in range(len(df)):
            df_image = df.iloc[i]
            image_fname = df_image["PATH"]
            file_type = image_fname.split(".")[-1]
            with tf.io.gfile.GFile(image_fname, "rb") as f:
                image_data = f.read()
            example = image_to_tfexample(
                str(df_image["SAMPLE_ID"]).encode("utf_8"),
                image_data,
                file_type.encode("utf_8"),
                df_image["HEIGHT"],
                df_image["WIDTH"],
                df_image["TEXT"].encode("utf_8"),
            )
            tfrecord_writer.write(example.SerializeToString())


def run_inference(df, output_folder, out_name):
    start_clip = time.time()

    filtered_df, img_embeddings = df_clipfilter(df)
    filtered_df.to_csv(output_folder + out_name + ".csv", index=False, sep="|")

    end_clip = time.time()
    print(f"[crawling@home] CLIP filtered {len(df)} in {round(end_clip - start_clip)} seconds")
    print(f"[crawling@home] CLIP efficiency {len(df) / (end_clip - start_clip)} img/sec")

    img_embeds_sampleid = {}
    for i, img_embed_it in enumerate(img_embeddings):
        dfid_index = filtered_df.at[i, "SAMPLE_ID"]
        img_embeds_sampleid[str(dfid_index)] = img_embed_it

    with open(f"{output_folder}image_embedding_dict-{out_name}.pkl", "wb") as f:
        pickle.dump(img_embeds_sampleid, f)

    df_tfrecords(
        filtered_df,
        f"{output_folder}crawling_at_home_{out_name}__00000-of-00001.tfrecord",
    )

    return len(filtered_df)
