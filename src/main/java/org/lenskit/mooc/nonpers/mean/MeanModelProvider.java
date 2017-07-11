package org.lenskit.mooc.nonpers.mean;

import com.google.common.base.MoreObjects;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.lenskit.data.dao.DataAccessObject;
import org.lenskit.data.ratings.Rating;
import org.lenskit.util.io.ObjectStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * @author DimaS
 */
class MeanModelProvider {
    private static final Logger logger = LoggerFactory.getLogger(DampedItemMeanModelProvider.class);

    static ItemMeanModel getMeanModel(DataAccessObject dao,
                                      BiFunction<Long, Double, Double> meanFunc,
                                      Consumer<Rating> ratingConsumer) {
        Long2ObjectMap<ItemRatingTotal> rMap = new Long2ObjectOpenHashMap<>();

        try (ObjectStream<Rating> ratings = dao.query(Rating.class).stream()) {
            for (Rating r: ratings) {
                if (!Double.isNaN(r.getValue())) {
                    ItemRatingTotal rt = rMap.computeIfAbsent(r.getItemId(), ItemRatingTotal::new);
                    rt.count++;
                    rt.total += r.getValue();
                    rMap.put(rt.itemId, rt);
                    if (ratingConsumer != null)
                        ratingConsumer.accept(r);
                }
            }
        }

        Long2DoubleOpenHashMap means = new Long2DoubleOpenHashMap();
        rMap.forEach((id, rt) -> means.put(id.longValue(), meanFunc.apply(rt.count, rt.total).doubleValue()));

        logger.info("computed mean ratings for {} items", means.size());
        return new ItemMeanModel(means);
    }

    /**
     * @author DimaS
     */
    private static class ItemRatingTotal {
        private final long itemId;
        private double total;
        private long count;

        private ItemRatingTotal(long itemId) {
            this.itemId = itemId;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("itemId", itemId)
                    .add("total", total)
                    .add("count", count)
                    .toString();
        }
    }
}
