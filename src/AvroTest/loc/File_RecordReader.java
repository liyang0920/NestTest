package AvroTest.loc;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class File_RecordReader extends RecordReader<NullWritable, Path>{
	private FileSplit fileSplit;
	private Path value;
	private boolean end = false;
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;
	}
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException{
		if (!end) {
			value = this.fileSplit.getPath();
			end = !end;
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return NullWritable.get();
	}
	@Override
	public Path getCurrentValue() throws IOException, InterruptedException {
		return this.value;
	}
	
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return end ? 1.0f : 0.0f;
	}
	
	@Override
	public void close() throws IOException {
		//do nothing
	}

}
