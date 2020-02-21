package com.sming.test;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CourseScore implements WritableComparable<CourseScore> {

	private String course;
	private String name;
	private double score;

	public CourseScore(String course, String name, double score) {
		super();
		this.course = course;
		this.name = name;
		this.score = score;
	}

	public CourseScore() {
	}

	public String getCourse() {
		return course;
	}

	public void setCourse(String course) {
		this.course = course;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	/**
	 * 排序规则 compareTo方法既充当排序用，用充当分组规则
	 */
	
	@Override
	public String toString() {
		return course + "\t" + name + "\t" + score;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(course);
		out.writeUTF(name);
		out.writeDouble(score);

	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.course = in.readUTF();
		this.name = in.readUTF();
		this.score = in.readDouble();
	}

	public int compareTo(CourseScore cs) {
		int courseDiff = this.course.compareTo(cs.getCourse());

		if (courseDiff == 0) {
			double diff = cs.getScore() - this.score;
			if (diff == 0) {
				return 0;
			} else {
				return diff > 0 ? 1 : -1;
			}
		} else {
			return courseDiff > 0 ? 1 : -1;
		}
	}
}
